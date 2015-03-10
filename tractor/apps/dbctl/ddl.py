"""This module has a ddl() function which outputs the SQL necessary to create the database on a postgresql server."""

import os
import tractor.base.EngineDB as EngineDB

ROLES_BY_BASE_ROLE = {
    "writeroles": ["bootstrap", "spooler", "dispatcher", "dev_write"],
    "readroles":  ["reader", "dashboard", "dev_read"]
    }

TABLE_OWNER = "dispatcher" # keep in sync with TractorPartitionCreate()

# boilerplate postgres setup, not specific to tractor database

PREAMBLE = r"""
SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = public, pg_catalog;
"""


DEFAULT_DATABASE_NAME = "tractor"

def ddl(dbname=DEFAULT_DATABASE_NAME):
    import upgrade
    db = EngineDB.EngineDB(db=dbname)
    # the first parts are executed before the database has been switched to "tractor"
    parts = [PREAMBLE, db.getCreate()]
    # create base (non-login) and login roles which inherit permissions from archtype base roles
    for baseRole, loginRoles in ROLES_BY_BASE_ROLE.iteritems():
        parts.append("CREATE ROLE %s;" % baseRole)
        for loginRole in loginRoles:
            parts.append("CREATE ROLE %s WITH LOGIN IN ROLE %s;" % (loginRole, baseRole))
    # grant permissions to base roles
    tableStr = ",".join([table.tablename.lower() for table in db.Tables])
    parts.extend([
    "GRANT SELECT,DELETE,INSERT,UPDATE ON %s TO writeroles;" % tableStr,
    "GRANT SELECT ON %s TO readroles;" % tableStr
    ])
    # grant permission to views
    viewStr = ",".join([view.name for view in db.Views])
    parts.extend([
    "GRANT SELECT ON %s TO writeroles;" % viewStr,
    "GRANT SELECT ON %s TO readroles;" % viewStr
    ])
    # limiting to one boostrap connection prevents multiple engines from running at once
    parts.append("ALTER ROLE bootstrap CONNECTION LIMIT 1;")
    # change the table owner to a role that can create inherited tables
    for table in db.Tables:
        parts.append("ALTER TABLE %s OWNER TO %s;" % (table.tablename.lower(), TABLE_OWNER))
    parts.extend([
    # start numbering jobs at 1
    "INSERT INTO param VALUES ('jidcounter', 0);",
    # default install limits result set size
    "INSERT INTO param VALUES ('maxrecords', 10000);",
    # default to archiving deleted jobs
    "INSERT INTO param VALUES ('archiving', 1);",
    # default to archiving deleted jobs
    "INSERT INTO param VALUES ('schema-version', '%s');" % upgrade.SCHEMA_VERSION,
    # the high-level plpython functions defined above
    ])
    return "\n".join(parts)

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(prog="ddl.py")
    parser.add_argument("--functions", action="store_true", help="dump only function definitions")
    parser.add_argument("--views", action="store_true", help="dump only view definitions")
    args = parser.parse_args()

    if args.functions:
        db = EngineDB.EngineDB(db=DEFAULT_DATABASE_NAME)
        for f in db.Functions:
            print f.getCreate()
    elif args.views:
        import upgrade
        for view in EngineDB.EngineDB.Views:
            print view.getCreate(), ";"
    else:
        print ddl()
