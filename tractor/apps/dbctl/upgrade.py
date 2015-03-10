"""This module provides classes to represent modifications to the DDL of the database
so that upgrades to the schema can be accurately represented."""

import tractor.base.EngineDB as EngineDB

# an ordered list of versions.  position, not name, matters.  newer versions appear at the end of the list.
SCHEMA_VERSIONS = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
    "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
    "61", "62", "63", "64", "65", "66"
    ]


# the current schema version
SCHEMA_VERSION = SCHEMA_VERSIONS[-1]

class Upgrade(object):
    """An abstract class that ensures protocol is followed."""
    def getSQL(self):
        raise UpgradeException, "Upgrade.getSQL(): subclass must override this method."

    def __str__(self):
        return self.getSQL()
    

class UpgradeWithSQL(Upgrade):
    def __init__(self, sql):
        self.sql = sql
        
    def getSQL(self):
        return self.sql


class CreateAllFunctions(Upgrade):
    def getSQL(self):
        return "\n".join([f.getCreate() for f in EngineDB.EngineDB.Functions])
    
    def __str__(self):
        return "Create all functions."

class DropAllFunctions(Upgrade):
    def getSQL(self):
        # a tractordummy() function is created to ensure that the EXECUTE
        # gets at least one statement to run; this is more relevant
        # during development when there are no tractor functions defined
        return """
DO $$
BEGIN
CREATE OR REPLACE FUNCTION public.tractordummy() RETURNS int LANGUAGE sql AS $dummy$ select 0; $dummy$;
EXECUTE (
    SELECT string_agg('DROP FUNCTION IF EXISTS ' || ns.nspname || '.' || proname || '(' || oidvectortypes(proargtypes) || ');', ' ')
       FROM  pg_proc INNER JOIN pg_namespace ns ON (pg_proc.pronamespace = ns.oid)
       WHERE proname LIKE 'tractor%'
       );
END
$$;
"""
    
    def __str__(self):
        return "Drop all functions."


class CreateAllViews(Upgrade):
    def getSQL(self):
        parts = ["DROP VIEW IF EXISTS %s;\n%s;\nGRANT SELECT ON %s TO readroles, writeroles;\n"
                 % (view.name, view.getCreate(), view.name) for view in EngineDB.EngineDB.Views]
        return "".join(parts)

    def __str__(self):
        return "Create all views."

class DropAllViews(Upgrade):
    def getSQL(self):
        # a dummyview is created to ensure that the EXECUTE
        # gets at least one statement to run; this is more relevant
        # during development when there are no tractor views defined
        return """
DO $$
BEGIN
CREATE OR REPLACE VIEW public.dummyview AS SELECT 0;
EXECUTE (
    SELECT string_agg('DROP VIEW IF EXISTS ' || t.oid::regclass || ';', ' ')
       FROM   pg_class t
       JOIN   pg_namespace n ON n.oid = t.relnamespace
       WHERE  t.relkind = 'v'
       AND    n.nspname = 'public'
       );
END
$$;
"""

    def __str__(self):
        return "Drop all views."
    
class UpgradeAddColumn(Upgrade):
    def __init__(self, table=None, column=None, coltype=None, default=None):
        self.table = table
        self.column = column
        self.coltype = coltype
        self.default = "DEFAULT %s" % default if default is not None else ""

    def getSQL(self):
        return "ALTER TABLE %s ADD COLUMN %s %s %s" % (self.table, self.column, self.coltype, self.default)


class UpgradeAddIndex(Upgrade):
    def __init__(self, table=None, column=None):
        self.table = table
        self.column = column

    def getSQL(self):
        return "CREATE INDEX %s_%s_index (%s)" % (self.table, self.column, self.column)
        

# mapping of schema version to the actions required to upgrade from prior version; maintain it!
UPGRADES_BY_VERSION = {
    "3": [UpgradeAddColumn(table="invocation", column="limits", coltype="text[]")],
    "4": [
        UpgradeWithSQL("ALTER ROLE dispatch RENAME TO dispatcher"),
        UpgradeWithSQL("ALTER ROLE query RENAME TO reader")
        ],
    "8": [
        UpgradeWithSQL(
            "ALTER TABLE job ALTER COLUMN project SET DATA TYPE text[] USING "
            "CASE project WHEN '' THEN '{}' WHEN NULL THEN '{}' ELSE ARRAY[project] END"),
        UpgradeWithSQL("ALTER TABLE job RENAME project TO projects")
        ],
    # add new columns to invocation table for tracking process stats
    "10": [
        UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN mem TO vsz"),
        UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN utime TO elapsedapp"),
        UpgradeWithSQL("ALTER TABLE invocation RENAME COLUMN stime TO elapsedsys"),
        UpgradeAddColumn(table="invocation", column="elapsedreal", coltype="real", default="0.0")
        ],
    # tags were getting encoded/decoded irregularly; retrofit existing values so they are compatible with new decoder
    "11": [UpgradeWithSQL("UPDATE job SET tags=STRING_TO_ARRAY(ARRAY_TO_STRING(tags, ''), ' ')")],
    # 12: new jobinfo view makes job info 10x faster than plpyutil function
    # 13: added spooldate to jobinfo view
    # 14: forgot to grant permissions on jobinfo view after rebuilding
    # 17: renamed command's refid column to refersto to be consistent with job syntax
    "17": [UpgradeWithSQL("ALTER TABLE command RENAME COLUMN refid TO refersto")],
    # 18: added refersto for taskdetails response
    # 19: update functions to remove debugging output, and remove deprecated function
    # 20: added delaytime to jobinfo view
    # 22: rename delaytime to aftertime
    "22": [ UpgradeWithSQL("ALTER TABLE job RENAME COLUMN delaytime TO aftertime")],
    # 23: remove the host field from the blade table
    "23": [UpgradeWithSQL("ALTER TABLE blade DROP COLUMN host")],
    # 25: at some point in the past, the bladeuse view was redefined to use ONLY to focus
    # on non-deleted jobs; some testing databases weren't updated.
    # 26: TractorSelect now encodes traceback as its result for errors instead of using plpy.error
    # 27: TractorJSONTaskDetails now has special logic for skipped tasks
    # 28: TractorJobinfoFile now includes job postscript commands (tid=0) in job-level cids
    # 29: add command state to TractorCmdlistFile so that TractorStateFile is no longer required
    # 30: move cids logic from TractorTasktreeFile to TractorJSONJtree
    # 31: added missing ONLY to restrict searches on a number of functions
    # fixed TractorJSONTaskDetails and TractorCmdlistFile to return commands
    # for those whose last invocation is not current, such as for retried task
    # whose commands have not yet started
    # 32: ensure that job-level clean-up commands are enumerated correctly
    # 33: added maxactive attribute to job and the jobinfo view
    "33": [UpgradeAddColumn(table="job", column="maxactive", coltype="integer", default="0")],
    # 34: update task progress when starting or stopping commands 
    # 35: update task details to be more robust and handle multi-blade commands
    # 36: set task progress to zero when a command finishes if it's not the last command
    # 37: return commands in blade info
    # 38: return serialsubtasks, atleast, atmost for tasks
    # 39: include progress in jtree
    # 40: include retryrc in cmdlist
    # 41: include task id and cmd id & refersto in task and command functions
    # 42: add osname and osversion columns to blade table
    "42": [
        UpgradeAddColumn(table="blade", column="osname", coltype="text"),
        UpgradeAddColumn(table="blade", column="osversion", coltype="text")
        ],
    # 43: include timestamps in jtree to support dashboard elapsed column
    # 44: add serialsubtasks to job table and jobinfo view
    "44": [
        UpgradeAddColumn(table="job", column="serialsubtasks", coltype="BOOLEAN", default="false"),
        ],
    # 45: include minSlots/maxSlots in cmdlist
    # 46: add osname and osversion to blade list and details 
    # 47: add boottime to blade details
    # 48: add dirmap to job table and jobinfo view
    "48": [UpgradeAddColumn(table="job", column="dirmap", coltype="JSON")],
    # 49: add dirmap to jobinfo view; ensure valule is json serialized only once
    # 50: add TractorPurgeArchiveToYearMonth for aging the archive tables
    # 51: add boottime to blade details
    # 52: add details to jtree response to avoid round trips to engine for dashboard rollover
    # 53: restricted bladeuse view to consider only current invocations
    # 54: TractorJSONBladeInfo to consider only current invocations
    # 55: redefined slotuse view and blade functions to report slotsinuse, not # active tasks
    # 56: added process stats to command details
    # 57: added paused to jobinfo view, independent of priority
    # 58: added support for task resuming
    "58": [
        UpgradeAddColumn(table="task", column="resumeblock", coltype="BOOLEAN", default="false"),
        UpgradeAddColumn(table="task", column="retrycount", coltype="INT", default="1"),
        UpgradeAddColumn(table="command", column="resumewhile", coltype="text[]"),
        UpgradeAddColumn(table="command", column="resumepin", coltype="BOOLEAN", default="false"),
        UpgradeAddColumn(table="invocation", column="retrycount", coltype="INT"),
        UpgradeAddColumn(table="invocation", column="resumecount", coltype="INT"),
        UpgradeAddColumn(table="invocation", column="resumable", coltype="BOOLEAN"),
        ],
    # 59: added "resuming" parameter to TractorTasksRetry
    # 60: updated TractorJSONJtree to indicate whether task is resumable
    # 61: updated plpyutil:cmdlistForRows to include retryrcodes as retryrc
    # 62: disabled nestloop algorithm for query planner; added sys* views for tracking locks
    # 63: updated TractorJSONJtree to add list of resumable cids to task
    # 64: updated TractorCmdlistFile to add blade to commands with resumepin=1
    # 65: start retrycount at 0 instead of 1
    "65": [UpgradeWithSQL("ALTER TABLE task ALTER COLUMN retrycount SET DEFAULT 0")],
    # 66: updated TractorJobinfoFile, return state=Done when numdone==numtasks
    }

def getUpgrades(fromVersion, toVersion):
    """Returns the upgrade objects  necessary to from the given version to the other version."""

    # upgrades will consist of
    # 1. dropping all views and functions
    # 2. performing SQL updates
    # 3. creating all views and functions
    
    if fromVersion == toVersion:
        return []

    upgrades = [DropAllViews(), DropAllFunctions()]
    # gets set to True once interator is in window between versions
    inWindow = False 
    for version in SCHEMA_VERSIONS:
        if inWindow:
            # upgrade version is inside target version window
            upgradesForVersion = UPGRADES_BY_VERSION.get(version, [])
            upgrades.extend(upgradesForVersion)
            # stop if loop has reached destination version
            if version == toVersion:
                break # for version
        elif version == fromVersion:
            inWindow = True
        elif version == toVersion:
            break # toVersion must be less than fromVersion to get here
    upgrades.extend([CreateAllViews(), CreateAllFunctions()])
    return upgrades
