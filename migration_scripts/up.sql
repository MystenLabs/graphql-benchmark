CREATE TABLE tx_sequence_numbers (
    tx_sequence_number bigint NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (tx_sequence_number)
);

CREATE TABLE tx_calls_cp (
    tx_sequence_number bigint NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    package bytea NOT NULL,
    module text NOT NULL,
    func text NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (package, module, func, address, tx_sequence_number)
);
CREATE INDEX pkg_tx ON tx_calls_cp (package, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_addr_tx ON tx_calls_cp (package, address, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_addr_rel_tx ON tx_calls_cp (package, address, rel, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_tx ON tx_calls_cp (package, module, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_addr_tx ON tx_calls_cp (package, module, address, tx_sequence_number, checkpoint_sequence_number);
CREATE INDEX pkg_mod_addr_rel_tx ON tx_calls_cp (package, module, address, rel, tx_sequence_number, checkpoint_sequence_number);


CREATE TABLE tx_senders_cp (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (address, tx_sequence_number, checkpoint_sequence_number)
);
CREATE INDEX tx_seq_num_senders ON tx_senders_cp (tx_sequence_number);

CREATE TABLE tx_recipients_cp (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    PRIMARY KEY (address, tx_sequence_number, checkpoint_sequence_number)
);
CREATE INDEX tx_seq_num_recipients ON tx_recipients_cp (tx_sequence_number);
# these are needed to speed up the migration for other tx_ lookup tables


CREATE TABLE tx_addresses (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (address, rel, tx_sequence_number, checkpoint_sequence_number)
);

CREATE TABLE tx_changed_objects_cp (
    tx_sequence_number bigint NOT NULL,
    object_id bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL
);


CREATE TABLE tx_input_objects_cp (
    tx_sequence_number bigint NOT NULL,
    object_id bytea NOT NULL,
    checkpoint_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (object_id, address, tx_sequence_number, checkpoint_sequence_number)
)
