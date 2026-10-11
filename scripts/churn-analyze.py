#!/usr/bin/env python3
"""Analyse churn.json (from the `churn` bin) against the extracted statements.
usage: churn-analyze.py <workdir> <tag> [<tag> ...]  -> markdown tables on stdout, JSON in <workdir>/analysis.json
"""
import difflib, json, re, sys
from collections import Counter, defaultdict
from pathlib import Path

W = Path(sys.argv[1]); tags = sys.argv[2:]
churn = json.loads((W / "churn.json").read_text())
stm = {t: json.loads((W / f"{t}.stmts.json").read_text()) for t in tags}
ids = {t: [x.strip() for x in (W / f"{t}.log.ids").read_text().split("\n")[:-1]] for t in tags}
sidx = {t: [int(x) for x in (W / f"{t}.stmt").read_text().split()] for t in tags}
def toks(m): return m.split()
def sim(a, b): return difflib.SequenceMatcher(None, toks(a), toks(b)).ratio()

rows, summary = [], defaultdict(int)
for n, t in enumerate(tags):
    c = churn[n]
    before = {i: (fp, tx) for i, fp, tx, *_ in c["before"]}
    lin_b = {i: l for i, _, _, l in c["before"]}; lin_a = {i: l for i, _, _, l in c["after"]}
    after = {i: (fp, tx) for i, fp, tx, *_ in c["after"]}
    ev = {i: k for i, k in c["events"]}
    total = sum(ev.values())
    active = len(ev)
    new_ids = set(c["new_ids"])
    widened = {i for i in before if i in after and before[i][0] != after[i][0]}
    # issue identity = what the Sentry forwarder groups on: fingerprint of current text (old) vs lineage id (new)
    id_changed_fp = widened
    id_changed_lin = {i for i in before if i in after and lin_b[i] != lin_a[i]}
    r = dict(release=t, statements=len(stm[t]), lines=c["lines"], untemplated=c["untemplated"],
             catalog=len(after), active=active, new=len(new_ids), widened=len(widened))
    if n == 0:
        r.update(baseline=True); rows.append(r); continue
    # statement-level ground truth vs previous release
    prev = stm[tags[n - 1]]
    pk = {(s["level"], s["msg"], tuple(s["tags"])) for s in prev}
    pm = {(s["level"], s["msg"]) for s in prev}
    cat = {}
    removed = [s for s in prev if (s["level"], s["msg"], tuple(s["tags"])) not in
               {(x["level"], x["msg"], tuple(x["tags"])) for x in stm[t]}]
    removed_pool = [s for s in removed if (s["level"], s["msg"]) not in {(x["level"], x["msg"]) for x in stm[t]}]
    for i, s in enumerate(stm[t]):
        k = (s["level"], s["msg"], tuple(s["tags"]))
        if k in pk: cat[i] = "unchanged"
        elif (s["level"], s["msg"]) in pm: cat[i] = "retagged"
        else:
            cand = [(sim(s["msg"], p["msg"]), p) for p in removed_pool if p["file"] == s["file"] and p["level"] == s["level"]]
            best = max(cand, key=lambda x: x[0], default=(0, None))
            cat[i] = "edited" if best[0] >= 0.5 else "added"
    cc = Counter(cat.values())
    r.update(stmt_unchanged=cc["unchanged"], stmt_retagged=cc["retagged"], stmt_edited=cc["edited"],
             stmt_added=cc["added"], stmt_removed=len(removed))
    # classify each new id by its statements
    per_id = defaultdict(Counter)
    for tid, si in zip(ids[t], sidx[t]):
        if tid != "-": per_id[int(tid)][cat[si]] += 1
    kind = Counter()
    new_kind = {}
    for i in new_ids:
        k = per_id[i].most_common(1)[0][0] if per_id[i] else "unknown"
        new_kind[i] = k; kind[k] += 1
    r.update(new_from_edited=kind["edited"], new_from_added=kind["added"], new_from_retagged=kind["retagged"],
             new_from_unchanged=kind["unchanged"])
    r.update(ident_changed_ids_fp=len(id_changed_fp), ident_changed_events_fp=sum(ev.get(i, 0) for i in id_changed_fp),
             ident_changed_ids_lin=len(id_changed_lin), ident_changed_events_lin=sum(ev.get(i, 0) for i in id_changed_lin))
    # events
    ev_new = sum(ev.get(i, 0) for i in new_ids)
    ev_wid = sum(ev.get(i, 0) for i in widened)
    ev_false = sum(ev.get(i, 0) for i in new_ids if new_kind[i] in ('edited', 'retagged')) + ev_wid
    r.update(false_novel_ids=sum(1 for i in new_ids if new_kind[i] in ('edited', 'retagged')) + len(widened), events_on_false_novelty=ev_false, false_novelty_event_share=ev_false / total, new_share_false=(sum(1 for i in new_ids if new_kind[i] in ('edited','retagged')) + len(widened)) / max(1, len(new_ids) + len(widened)),
             events=total, events_on_new=ev_new, events_on_widened=ev_wid,
             events_on_changed_id=ev_new + ev_wid, shape_coverage_retained=(total - ev_new - ev_wid) / total)
    # id (fingerprint) changes among templates live last release
    prev_active = {int(i) for i in set(ids[tags[n - 1]]) if i != "-"}
    prev_fps = {before[i][0] for i in prev_active if i in before}
    gone = [i for i in prev_active if before[i][0] not in {v[0] for v in after.values()}]
    r.update(prev_active_fp_lost=len(gone), prev_active=len(prev_active))
    # lineage attempt: link new+widened-successor templates to a lost predecessor by token edit similarity
    lost_text = {i: before[i][1] for i in before if ev.get(i, 0) == 0 or i in widened}
    succ = sorted(new_ids | widened)
    linked = ok = wrong_on_added = added_n = edit_n = 0
    # ground truth ancestor of an edited statement: template id (prev release) of its paired removed stmt
    prev_tid_of_stmt = {}
    for tid, si in zip(ids[tags[n - 1]], sidx[tags[n - 1]]):
        if tid != "-": prev_tid_of_stmt.setdefault(si, int(tid))
    for i in new_ids:
        k = new_kind[i]
        txt = after[i][1]
        best = max(((sim(txt, tx), j) for j, tx in lost_text.items()), default=(0, None))
        hit = best[0] >= 0.6
        if k == "edited":
            edit_n += 1
            if hit:
                linked += 1
                # correct if the linked predecessor's statements include one that was removed and paired in same file
                pj = best[1]
                psts = [prev[si] for si, tid in prev_tid_of_stmt.items() if tid == pj]
                cur = [stm[t][si] for tid, si in zip(ids[t], sidx[t]) if tid != "-" and int(tid) == i]
                if any(p in removed_pool and any(p["file"] == q["file"] and sim(p["msg"], q["msg"]) >= 0.5 for q in cur) for p in psts):
                    ok += 1
        elif k == "added":
            added_n += 1
            if hit: wrong_on_added += 1
    r.update(lineage_edit_new=edit_n, lineage_linked=linked, lineage_correct=ok,
             lineage_added_new=added_n, lineage_false_link=wrong_on_added)
    rows.append(r)
(W / "analysis.json").write_text(json.dumps(rows, indent=1))

tr = rows[1:]
S = lambda k: sum(x[k] for x in tr)
print("| release | statements | lines | catalog | active | new | widened | events on changed id | coverage retained |\n|---|---|---|---|---|---|---|---|---|")
for x in rows:
    if x.get("baseline"): print(f"| {x['release']} (baseline) | {x['statements']} | {x['lines']} | {x['catalog']} | {x['active']} | {x['new']} (seed) | - | - | - |")
    else: print(f"| {x['release']} | {x['statements']} | {x['lines']} | {x['catalog']} | {x['active']} | {x['new']} | {x['widened']} | {x['events_on_changed_id']} / {x['events']} | {x['shape_coverage_retained']:.4f} |")
print("\n| release | stmts unchanged | retagged | edited | added | removed | new ids from edited | from added | from retagged | prev-active ids whose fingerprint is lost |\n|---|---|---|---|---|---|---|---|---|---|")
for x in tr: print(f"| {x['release']} | {x['stmt_unchanged']} | {x['stmt_retagged']} | {x['stmt_edited']} | {x['stmt_added']} | {x['stmt_removed']} | {x['new_from_edited']} | {x['new_from_added']} | {x['new_from_retagged']} | {x['prev_active_fp_lost']} / {x['prev_active']} |")
print("\n| release | false-novelty ids (edited + retagged + widened) | share of new+widened ids | events on them | share of events |\n|---|---|---|---|---|")
for x in tr: print(f"| {x['release']} | {x['false_novelty_ids'] if 'false_novelty_ids' in x else x['false_novel_ids']} | {x['new_share_false']:.3f} | {x['events_on_false_novelty']} | {x['false_novelty_event_share']:.4f} |")
print("\n| release | edited-origin new ids | linked by edit-distance (>=0.6) | linked correctly | genuinely-added new ids | of which falsely linked |\n|---|---|---|---|---|---|")
for x in tr: print(f"| {x['release']} | {x['lineage_edit_new']} | {x['lineage_linked']} | {x['lineage_correct']} | {x['lineage_added_new']} | {x['lineage_false_link']} |")
print(f"\nTOTAL false_ids={S('false_novel_ids')} false_events={S('events_on_false_novelty')} max_false_event_share={max(x['false_novelty_event_share'] for x in tr):.4f} max_false_id_share={max(x['new_share_false'] for x in tr):.3f}")
print(f"TOTAL transitions={len(tr)} new={S('new')} widened={S('widened')} events={S('events')} changed_events={S('events_on_changed_id')} "
      f"new_from_edited={S('new_from_edited')} new_from_added={S('new_from_added')} new_from_retagged={S('new_from_retagged')} "
      f"lineage_edit={S('lineage_edit_new')} linked={S('lineage_linked')} correct={S('lineage_correct')} added={S('lineage_added_new')} falselink={S('lineage_false_link')} "
      f"mean_cov={sum(x['shape_coverage_retained'] for x in tr)/len(tr):.4f} min_cov={min(x['shape_coverage_retained'] for x in tr):.4f}")
print("\n| release | widened ids: identity changed (text fingerprint) | events | widened ids: identity changed (lineage id) | events |\n|---|---|---|---|---|")
for x in tr: print(f"| {x['release']} | {x['ident_changed_ids_fp']} | {x['ident_changed_events_fp']} | {x['ident_changed_ids_lin']} | {x['ident_changed_events_lin']} |")
print(f"TOTAL identity-changed ids fp={S('ident_changed_ids_fp')} events fp={S('ident_changed_events_fp')} ids lineage={S('ident_changed_ids_lin')} events lineage={S('ident_changed_events_lin')} of events={S('events')}")
