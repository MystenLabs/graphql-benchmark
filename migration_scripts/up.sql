CREATE TABLE tx_calls_rel (
    tx_sequence_number bigint NOT NULL,
    cp_sequence_number bigint NOT NULL,
    package bytea NOT NULL,
    module text NOT NULL,
    func text NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (package, module, func, address, tx_sequence_number)
);
create index pkg_tx on tx_calls_rel (package, tx_sequence_number);
create index pkg_mod_tx on tx_calls_rel (package, module, tx_sequence_number);
create index pkg_addr_tx_rel on tx_calls_rel (package, address, tx_sequence_number, rel);
create index pkg_mod_addr_tx_rel on tx_calls_rel (package, module, address, tx_sequence_number, rel);

CREATE TABLE tx_addresses (
    tx_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    cp_sequence_number bigint NOT NULL,
    rel smallint NOT NULL,
    PRIMARY KEY (address, tx_sequence_number, cp_sequence_number)
);

CREATE TABLE tx_changed_objects_rel (
    tx_sequence_number bigint NOT NULL,
    object_id bytea NOT NULL,
    cp_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL
);


CREATE TABLE tx_input_objects_rel (
    tx_sequence_number bigint NOT NULL,
    object_id bytea NOT NULL,
    cp_sequence_number bigint NOT NULL,
    address bytea NOT NULL,
    rel smallint NOT NULL
);
alter table tx_input_objects_rel set (autovacuum_enabled = false);

create table tx_recipients_rel (
tx_sequence_number bigint not null,
cp_sequence_number bigint not null,
recipient bytea not null,
transaction_kind smallint not null
);
