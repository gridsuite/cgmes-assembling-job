CREATE TABLE IF NOT EXISTS cgmes_assembling.handled_files (
    filename text,
    origin text,
    handled_date timestamp,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS cgmes_assembling.filename_by_uuid (
    uuid text,
    filename text,
    origin text,
    PRIMARY KEY (uuid, origin)
);

CREATE TABLE IF NOT EXISTS cgmes_assembling.uuid_by_filename (
    filename text,
    uuid text,
    origin text,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS cgmes_assembling.imported_files (
    filename text,
    origin text,
    import_date timestamp,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS cgmes_assembling.dependencies (
    uuid text,
    dependencies frozen<List<text>>,
    PRIMARY KEY (uuid)
);
