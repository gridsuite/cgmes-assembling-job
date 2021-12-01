CREATE TABLE IF NOT EXISTS handled_files (
    filename text,
    origin text,
    handled_date timestamp,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS filename_by_uuid (
    uuid text,
    filename text,
    origin text,
    PRIMARY KEY (uuid, origin)
);

CREATE TABLE IF NOT EXISTS uuid_by_filename (
    filename text,
    uuid text,
    origin text,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS imported_files (
    filename text,
    origin text,
    import_date timestamp,
    PRIMARY KEY (filename, origin)
);

CREATE TABLE IF NOT EXISTS dependencies (
    uuid text,
    dependencies frozen<List<text>>,
    PRIMARY KEY (uuid)
);
