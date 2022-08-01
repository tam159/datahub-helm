import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Construct a lineage object.
lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn(platform="trino", name="hive.gfg_data_curated.theiconic_orders", env="PROD"),
        builder.make_dataset_urn(platform="trino", name="hive.gfg_data_curated.theiconic_items", env="PROD"),
        builder.make_dataset_urn(platform="trino", name="hive.gfg_data_curated.theiconic_customers", env="PROD"),
        builder.make_dataset_urn(platform="trino", name="hive.gfg_data_curated.theiconic_products", env="PROD"),
    ],
    builder.make_dataset_urn(platform="trino", name="hive.dbg_ti.theiconic_competitive_insights", env="PROD"),
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter("http://localhost:8080")

# Emit metadata!
emitter.emit_mce(lineage_mce)
