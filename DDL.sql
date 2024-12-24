CREATE TABLE statistics
(
    history_id SERIAL PRIMARY KEY,
    day date NOT NULL,
    price_zone int  NOT NULL,
    PCB_index float not null,
    percent_change varchar(8) NOT NULL,
    planed_volume_consumption float NOT NULL,
    percent_change_2 varchar(8) NOT NULL,
    planed_volume_prod_TES float NOT NULL,
    planed_volume_prod_GES float NOT NULL,
    planed_volume_prod_AES float NOT NULL,
    planed_volume_prod_SES_VES float NOT NULL \
);"

