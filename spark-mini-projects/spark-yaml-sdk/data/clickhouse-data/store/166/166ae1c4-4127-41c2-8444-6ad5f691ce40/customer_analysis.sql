ATTACH TABLE _ UUID '4ac3554d-1345-43e7-baae-899df58d4f53'
(
    `customer_id` Int32,
    `customer_name` String,
    `total_spent` Float64
)
ENGINE = MergeTree
ORDER BY customer_id
SETTINGS index_granularity = 8192
