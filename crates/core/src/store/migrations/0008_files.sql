-- File contents, stored once per distinct content.
CREATE TABLE blobs (
    id     INTEGER PRIMARY KEY,
    sha256 BLOB    NOT NULL UNIQUE,
    size   INTEGER NOT NULL,
    data   BLOB    NOT NULL
);

-- Parts a client uploaded (upload.saveFilePart / saveBigFilePart), until a call uses
-- the file. `file_id` is the client's random id for the upload.
CREATE TABLE upload_parts (
    owner_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    file_id  INTEGER NOT NULL,
    part     INTEGER NOT NULL,
    total    INTEGER,           -- big files declare their part count
    data     BLOB    NOT NULL,
    PRIMARY KEY (owner_id, file_id, part)
);

-- Photos and documents, as messages carry them (JSON-TL Photo / Document).
CREATE TABLE media (
    id          INTEGER PRIMARY KEY,   -- the photo's or document's id
    kind        TEXT    NOT NULL,      -- photo | document
    access_hash INTEGER NOT NULL,
    owner_id    INTEGER NOT NULL,      -- who uploaded it
    date        INTEGER NOT NULL,
    body        TEXT    NOT NULL
);

-- The files of a medium: a photo's sizes by type ("s", "m", "x"…), a document's file
-- ("") and its thumbnails.
CREATE TABLE media_files (
    media_id INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    thumb    TEXT    NOT NULL,
    blob_id  INTEGER NOT NULL REFERENCES blobs(id),
    PRIMARY KEY (media_id, thumb)
);
