import boto3
import logging
import json
import base64
import psycopg2
import argparse
import requests
import sys
import logging
import getpass

from neo4j import GraphDatabase

from botocore.exceptions import ClientError
from requests.packages.urllib3 import Retry

parser = argparse.ArgumentParser()
parser.add_argument('--redshift_dbname',type=str, help='Provide Redshift database name', required=True)
parser.add_argument('--redshift_host',type=str, help='Provide Redshift host', required=True)
parser.add_argument('--redshift_port',type=str, help='Provide Redshift port', required=True)
parser.add_argument('--neo4j_hostname',type=str, help='Provide Neo4j hostname', required=True)

args = parser.parse_args()

# Redshift view dependency class
class RedshiftViewDependency:

    # Connect to Redshift cluster
    def __init__(self, redshift_dbname, redshift_host, redshift_port, redshift_user, redshift_password):

        # Connect to Redshift
        try:
            self.conn = psycopg2.connect(dbname=redshift_dbname, host=redshift_host, port=redshift_port, user=redshift_user, password=redshift_password)
            logging.info('Connected to Redshift')

        except Exception as err:
            logging.error('%s' % err)
            sys.exit(1)

    # Get view dependency
    def get_view_dependency(self):

        viewDependencyLst = []

        sql = """
            select
                case
                    when src_obj.relkind = 'r' then 'table'
                    when src_obj.relkind = 'v' then 'view'
                end as src_object_type,
                v_depend.src_schemaname,
                v_depend.src_objectname,
                src_obj_info.relcreationtime as src_creation_time,
                case
                    when tgt_obj.relkind = 'r' then 'table'
                    when tgt_obj.relkind = 'v' then 'view'
                end as src_object_type,
                v_depend.dependent_schemaname,
                v_depend.dependent_objectname,
                src_obj_info.relcreationtime as tgt_creation_time,
           v_depend.dependent_viewoid
            from
                    (
                        select distinct
                             srcobj.oid AS src_oid
                            ,srcnsp.nspname AS src_schemaname
                            ,srcobj.relname AS src_objectname
                            ,tgtobj.oid AS dependent_viewoid
                            ,tgtnsp.nspname AS dependent_schemaname
                            ,tgtobj.relname AS dependent_objectname
                        from
                            pg_catalog.pg_class AS srcobj
                        inner join
                            pg_catalog.pg_depend AS srcdep
                                on srcobj.oid = srcdep.refobjid
                        inner join
                            pg_catalog.pg_depend AS tgtdep
                                on srcdep.objid = tgtdep.objid
                        join
                            pg_catalog.pg_class AS tgtobj
                                on tgtdep.refobjid = tgtobj.oid
                                and srcobj.oid <> tgtobj.oid
                        left outer join
                            pg_catalog.pg_namespace AS srcnsp
                                on srcobj.relnamespace = srcnsp.oid
                        left outer join
                            pg_catalog.pg_namespace tgtnsp
                                on tgtobj.relnamespace = tgtnsp.oid
                        where tgtdep.deptype = 'i' --dependency_internal
                        and tgtobj.relkind = 'v' --v=view

                    ) v_depend
            inner join
                    pg_class src_obj
            on
                v_depend.src_oid = src_obj.oid
            inner join
                pg_class_info src_obj_info
            on
                v_depend.src_oid = src_obj_info.reloid
            inner join
                    pg_class tgt_obj
            on
                v_depend.dependent_viewoid = tgt_obj.oid
            inner join
                pg_class_info tgt_obj_info
            on
                v_depend.dependent_viewoid = tgt_obj_info.reloid
            where
                v_depend.dependent_schemaname not in ('admin', 'information_schema', 'pg_catalog')
        """

        try:
            curs = self.conn.cursor()
            curs.execute(sql)

            for row in curs.fetchall():

                viewDependencyLst.append ({
                    'src_obj_type' : row[0],
                    'src_schema' : row[1],
                    'src_obj_name' : row[2],
                    'tgt_obj_type': row[4],
                    'tgt_schema': row[5],
                    'tgt_obj_name': row[6]
                })

        except Exception as err:
            logging.error('%s' % err)
            sys.exit(1)

        else:
            return viewDependencyLst

    def __del__(self):

        # Close Redshift connection
        try:
            self.conn.close()
        except Exception as err:
            logging.error('%s' % err)

        logging.info('Disconnected from Redshift')

# View  dependency lineage class
class ViewLineage:

    # Connect to Neptune cluster connection
    def __init__(self, neo4j_hostname, neo4j_username, neo4j_password):

        try:

            self.driver = GraphDatabase.driver("bolt://%s:7687" % neo4j_hostname, auth=(neo4j_username, neo4j_password))
            logging.info('Connected to Neo4j')

            # Open transaction
            self.tx = self.driver.session()#.begin_transaction()

        except Exception as err:
            logging.error('%s' % err)
            sys.exit(1)

    # Close connection
    def __del__(self):

        logging.info('Disconnected from Neo4j')
        self.driver.close()

    # Add dependency pair
    def add_pair (self, item):

        try:
            self.tx.run("""
                            MERGE (src:{src_type}{{type: '{src_type}' ,schema: '{src_schema}', name: '{src_name}', fullname: '{src_schema}.{src_name}'}})
                            MERGE (tgt:{tgt_type} {{type: '{tgt_type}' ,schema: '{tgt_schema}', name: '{tgt_name}', fullname: '{tgt_schema}.{tgt_name}'}})
                            MERGE (src)-[s2t:source_to_target]->(tgt)
                        """.format (src_type = item['src_obj_type'],
                               src_schema = item['src_schema'],
                               src_name = item['src_obj_name'],
                               tgt_type = item['tgt_obj_type'],
                               tgt_schema = item['tgt_schema'],
                               tgt_name = item['tgt_obj_name']
                               )
                        )

            logging.info('Lineage {src_schema}.{src_name}->{tgt_schema}.{tgt_name} created'.format (src_schema = item['src_schema'],
                                    src_name = item['src_obj_name'],
                                    tgt_schema=item['tgt_schema'],
                                    tgt_name=item['tgt_obj_name']
                        ))

        except Exception as err:
            logging.error('%s' % err)
            sys.exit(1)

    # Clear graph
    def clear_graph(self):

        try:
            # Cleanup existing Graph
            self.tx.run('MATCH (n) DETACH DELETE n')
        except Exception as err:
            logging.error('%s' % err)
            sys.exit(1)
        else:
            logging.info('Graph has been cleaned')

def main():

    # Request credentials
    neo4j_username = input("Neo4j username: ")
    neo4j_password = getpass.getpass()

    # Request credentials
    redshift_username = input("Redshift username: ")
    redshift_password = getpass.getpass()

    # Set logging configuration
    logging.basicConfig(format='%(asctime)s %(levelname)s %(lineno)d %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

    # Create RedshiftViewDependency class instance
    viewDependency = None
    viewDependency = RedshiftViewDependency(args.redshift_dbname, args.redshift_host, args.redshift_port, redshift_username, redshift_password).get_view_dependency()

    # If output exists
    if viewDependency is not None and len(viewDependency)>0:
        lineAge = ViewLineage(args.neo4j_hostname, neo4j_username, neo4j_password)
        lineAge.clear_graph()

        for row in viewDependency:

            # Add lineage pair
            lineAge.add_pair(row)

if __name__ == "__main__":

    main()


