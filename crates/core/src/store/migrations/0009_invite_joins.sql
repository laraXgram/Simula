-- Who joined through each invite link (an approved request counts, with its approver).
CREATE TABLE invite_joins (
    hash        TEXT    NOT NULL,
    user_id     INTEGER NOT NULL,
    date        INTEGER NOT NULL,
    approved_by INTEGER,
    PRIMARY KEY (hash, user_id)
);
