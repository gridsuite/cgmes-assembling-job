CREATE KEYSPACE IF NOT EXISTS "cgmes_assembling" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
CREATE KEYSPACE IF NOT EXISTS "merge_orchestrator" WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
