"""This module contains functions useful to plpython scripts."""

import time
import dateutil.parser
import rpg.timeutil as timeutil

RUNTYPE_CHAR_BY_NAME = {
    "cleanup": "D", "regular": "C",
    "post_always": "P", "post_error": "PE", "post_done": "PD"
}

CHAR_BY_STATE = {"active": "A", "blocked": "B", "ready": "B", "done": "D", "error": "E"}

def tasktreeForRows(rows):
    """Return a json string matching a 1.x on-disk representation of the tasktree file.
    Some additional values have been computed for use in q=jtree queries."""

    # allow rows to be indexed by tid
    rowByTid = {}
    for row in rows:
        rowByTid[row.get("tid", 0)] = row

    # set up children and antecedent references in each row
    for row in rows:
        ptids = row.get("ptids")
        if ptids:
            if ptids[0] != 0:
                # add this row to parent row's children
                parent = rowByTid.get(ptids[0])
                if parent:
                    parent.setdefault("children", []).append(row)
            # add tid as antecedent of remaining ptids
            for ptid in ptids[1:]:
                parent = rowByTid.get(ptid)
                if parent:
                    parent.setdefault("ants", []).append(row["tid"])

    def taskForRow(aRow):
        task = {
            "#": "T%d" % aRow["tid"],
            "children": [taskForRow(child) for child in aRow.get("children", [])],
            "data": {
                "tid": aRow["tid"],
                "title": aRow["title"],
                "state": ("+" if aRow.get("haslog") else "-") + CHAR_BY_STATE.get(aRow.get("state"), "B") +
                (",%d" % int(aRow.get("progress")) if aRow.get("progress") else ""),
                "cids": aRow.get("cids", [])
                }
            }

        # only add the following attributes if they have values
        if aRow["minslots"]:
            task["data"]["minSlots"] = aRow["minslots"]
        if aRow["maxslots"]:
            task["data"]["maxSlots"] = aRow["maxslots"]
        task["data"].update(
            dict([(k, aRow[k])
                  for k in ("id", "ants", "service", "preview", "chaser", "serialsubtasks",
                            "statetime", "activetime", "blade", "resumeblock")
                  if aRow.get(k)
                  ]))
        if aRow.get("rcode") is not None:
            task["data"]["rcode"] = aRow["rcode"]
        if aRow.get("rcids"):
            task["data"]["rcids"] = aRow.get("rcids")

        return task

    # compute list of children tasks
    children = []
    # march through tasks, starting with those that are root tasks, and recursively descending
    for row in rows:
        if row.get("ptids") and row["ptids"][0] == 0:
            children.append(taskForRow(row))

    tasktree = {"children": children}
    return tasktree

def cmdlistForRows(rows, invos):
    """Return a json string matching a 1.x on-disk representation of the command list."""

    cmdlist = {}
    
    invoByCid = {}
    for invo in invos:
        invoByCid[invo["cid"]] = invo
    
    for row in rows:
        invo = invoByCid.get(row.get("cid"), {})
        state = "B"
        if row["state"] == "done":
            # both done and skipped tasks have all command states marked as "D"
            state = "D"
        elif invo.get("current"):
            if invo.get("rcode"):
                state = "E"
            elif invo.get("stoptime"):
                state = "D"
            elif invo.get("starttime"):
                state = "A"
        cmd = {
            "argv": row.get("argv", []),
            "cid": row.get("cid", 0),
            "state": state,
            "service": row.get("service", ""),
            "msg": row.get("msg", ""),
            "type": ("L" if row.get("local") else "R") + \
            RUNTYPE_CHAR_BY_NAME.get(row.get("runtype", "regular"), "C") + ("X" if row.get("expand") else ""),
            "tags": row.get("tags", []),
            }
        cmd.update(
            dict([(k, row[k]) for k in ("envkey", "id", "refersto", "resumewhile", "resumepin") if row.get(k)]))
        if row["retryrcodes"]:
            cmd["retryrc"] = row["retryrcodes"]
        if row["minslots"]:
            cmd["minSlots"] = row["minslots"] # note capitalization
        if row["maxslots"]:
            cmd["maxSlots"] = row["maxslots"] # note capitalization
        # resumable commands that must resume to the same host should have the blade specified
        if row.get("resumepin") and invo.get("resumable") and invo.get("blade"):
            cmd["blade"] = invo.get("blade")
        cmdlist["C%d" % row.get("cid", 0)] = cmd

    return cmdlist

def taskDetailsForRows(cmdRows, invoRows):
    """Return a list of dictionaries representing the details for the given commands."""

    # the list of commands to be returned
    cmds = []

    # build a LUT to locate invocations by cid
    invoRowsByCid = {}
    for invoRow in invoRows:
        cid = invoRow.get("cid")
        invoRowsByCid.setdefault(cid, []).append(invoRow)

    # iterate over each command to generate a command entry
    for cmdRow in cmdRows:
        cid = cmdRow.get("cid")
        cmd = {}

        # iterate over invocations of the command to deteremine T1-compatible command-level attributes
        # (remember, multi-blade tasks can have multiple current invocations)

        # flags get set to True if any current invocations are in that state
        hasActive = False
        hasDone = False
        hasError = False
        invos = invoRowsByCid.get(cid, [])
        for invo in invos:
            if invo.get("t1"):
                if invo.get("rcode"):
                    hasError = True
                else:
                    hasDone = True
            elif invo.get("t0"):
                hasActive = True

        # deduce overall command state based on invocations
        if hasActive:
            state = "Active"
        elif hasError:
            state = "Error"
        elif hasDone:
            state = "Done"
        else:
            state = "Blocked"

        blades = [invo.get("blade") for invo in invos if invo.get("blade")]
        # take maximum rss, vsz, and cpu if multiple current invocations
        rss = max([invo.get("rss", 0) for invo in invos]) if invos else 0
        vsz = max([invo.get("vsz", 0) for invo in invos]) if invos else 0
        cpu = max([invo.get("cpu", 0) for invo in invos]) if invos else 0
        # take sum of elapsedapp/sys if multiple current invocations
        elapsedapp = sum([invo.get("elapsedapp", 0) for invo in invos])
        elapsedsys = sum([invo.get("elapsedsys", 0) for invo in invos])
        
        t0 = None # will be the minimum starttime of a command's invocations
        t1 = None # will be the maximum stoptime of a command's invocations, unless there are active ones
        for invo in invos:
            if invo["t0"] and (t0 is None or invo["t0"] < t0):
                t0 = invo["t0"]
            if invo["t1"] and (t1 is None or invo["t1"] > t1):
                t1 = invo["t1"]

        exitCode = None
        for invo in invos:
            if invo["rcode"] is not None:
                exitCode = exitCode or invo["rcode"] # a non-zero exit code trumps a zero exit code

        # deal with special case where task was skipped
        if not t0 and not t1 and cmdRow.get("state") == "done":
            state = "Skipped"
            t0 = cmdRow.get("statetimesecs")
            t1 = t0
            
        cmd = {
            "argv": cmdRow.get("argv", []),
            "cid": cid,
            "service": cmdRow.get("service", ""),
            "type": ("L" if cmdRow.get("local") else "R") + \
            RUNTYPE_CHAR_BY_NAME.get(cmdRow.get("runtype", "regular"), "C") + ("X" if cmdRow.get("expand") else ""),
            "tags": cmdRow.get("tags", []),
            "envkey": cmdRow.get("envkey", []),
            "blades": blades,
            "state": state,
            "exitcode": str(exitCode) if exitCode is not None else None,
            "t0": t0,
            "t1": t1,
            "elapsedapp": elapsedapp,
            "elapsedsys": elapsedsys,
            "rss": rss,
            "vsz": vsz,
            "cpu": cpu
            }
        
        # update certain fields only if they exist to reduce size of dictionary
        if cmdRow.get("cmdid"):
            cmd["id"] = cmdRow["cmdid"]
        if cmdRow.get("refersto"):
            cmd["refersto"] = cmdRow["refersto"]
        if cmdRow.get("minslots"):
            cmd["minSlots"] = cmdRow["minslots"] # note: capitalization
        if cmdRow.get("maxslots"):
            cmd["maxSlots"] = cmdRow["maxslots"] # note: capitalization

        # add cmd to list of commands returned
        cmds.append(cmd)

    return cmds

