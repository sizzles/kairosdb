//! `GET /api/v1/features[/{feature}]`: metadata describing the available
//! aggregators and group-bys, consumed by query-builder UIs. The shape
//! mirrors the Java `FeaturesResource` output (generated there from
//! `@FeatureComponent` annotations); property metadata here covers the
//! commonly used fields rather than every validation annotation.

use serde_json::{json, Value as JsonValue};

fn sampling_properties() -> Vec<JsonValue> {
    vec![
        json!({"name": "sampling", "label": "Sampling", "optional": false, "type": "Object",
               "properties": [
                   {"name": "value", "label": "Value", "type": "long", "default_value": "1"},
                   {"name": "unit", "label": "Unit", "type": "enum", "default_value": "milliseconds",
                    "options": ["milliseconds","seconds","minutes","hours","days","weeks","months","years"]}
               ]}),
        json!({"name": "align_sampling", "label": "Align sampling", "type": "boolean", "default_value": "true"}),
        json!({"name": "align_start_time", "label": "Align start time", "type": "boolean", "default_value": "false"}),
        json!({"name": "align_end_time", "label": "Align end time", "type": "boolean", "default_value": "false"}),
    ]
}

fn range_aggregator(name: &str, label: &str, description: &str) -> JsonValue {
    json!({"name": name, "label": label, "description": description, "properties": sampling_properties()})
}

fn pad_properties() -> Vec<JsonValue> {
    let mut props = sampling_properties();
    props.push(json!({"name": "pad_value", "label": "Pad value", "type": "long", "default_value": "0"}));
    props
}

fn percentile_properties() -> Vec<JsonValue> {
    let mut props = sampling_properties();
    props.push(json!({"name": "percentile", "label": "Percentile", "type": "double", "default_value": "0.1"}));
    props
}

fn simple_aggregator(name: &str, label: &str, description: &str, props: Vec<JsonValue>) -> JsonValue {
    json!({"name": name, "label": label, "description": description, "properties": props})
}

pub fn features() -> JsonValue {
    json!([
        {
            "name": "aggregators",
            "label": "Aggregator",
            "properties": [
                range_aggregator("avg", "AVG", "Averages the data points together."),
                range_aggregator("count", "COUNT", "Counts the number of data points."),
                range_aggregator("dev", "DEV", "Calculates the standard deviation of the time series."),
                simple_aggregator("diff", "DIFF", "Computes the difference between successive data points.", vec![]),
                simple_aggregator("div", "DIV", "Divides each data point by a divisor.",
                    vec![json!({"name": "divisor", "label": "Divisor", "type": "double"})]),
                simple_aggregator("filter", "FILTER", "Filters data points according to a filter operation.",
                    vec![json!({"name": "filter_op", "label": "Filter operation", "type": "enum",
                                "options": ["lte","lt","gte","gt","equal","ne"]}),
                         json!({"name": "threshold", "label": "Threshold", "type": "double"})]),
                range_aggregator("first", "FIRST", "Returns the first value data point for the time range."),
                range_aggregator("gaps", "GAPS", "Marks gaps in data according to sampling rate with a null data point."),
                range_aggregator("last", "LAST", "Returns the last value data point for the time range."),
                range_aggregator("least_squares", "LEAST_SQUARES", "Returns two points for the range which represent the best fit line through the set of points."),
                range_aggregator("max", "MAX", "Returns the maximum value data point for the time range."),
                range_aggregator("min", "MIN", "Returns the minimum value data point for the time range."),
                {
                    "name": "pad", "label": "PAD",
                    "description": "Pads empty ranges with a value.",
                    "properties": pad_properties()
                },
                {
                    "name": "percentile", "label": "PERCENTILE",
                    "description": "Finds the percentile of the data range.",
                    "properties": percentile_properties()
                },
                simple_aggregator("rate", "RATE", "Computes the rate of change for the data points.",
                    vec![json!({"name": "unit", "label": "Unit", "type": "enum", "default_value": "milliseconds",
                                "options": ["milliseconds","seconds","minutes","hours","days","weeks","months","years"]})]),
                simple_aggregator("sampler", "SAMPLER", "Computes the sampling rate of change for the data points.",
                    vec![json!({"name": "unit", "label": "Unit", "type": "enum", "default_value": "milliseconds",
                                "options": ["milliseconds","seconds","minutes","hours","days","weeks","months","years"]})]),
                simple_aggregator("save_as", "SAVE_AS", "Saves the results to another metric.",
                    vec![json!({"name": "metric_name", "label": "Save as", "type": "string"})]),
                simple_aggregator("scale", "SCALE", "Scales each data point by a factor.",
                    vec![json!({"name": "factor", "label": "Factor", "type": "double"})]),
                simple_aggregator("score", "SCORE", "Scores the data based on a set of thresholds.",
                    vec![json!({"name": "thresholds", "label": "Thresholds", "type": "array"}),
                         json!({"name": "order", "label": "Order", "type": "enum", "default_value": "ascending",
                                "options": ["ascending","descending"]})]),
                simple_aggregator("sma", "SMA", "Simple moving average.",
                    vec![json!({"name": "size", "label": "Size", "type": "int"})]),
                range_aggregator("sum", "SUM", "Adds data points together."),
                simple_aggregator("time_diff", "TIME_DIFF", "Computes the time difference between successive data points.",
                    vec![json!({"name": "time_unit", "label": "Time Unit", "type": "enum", "default_value": "seconds",
                                "options": ["milliseconds","seconds","minutes","hours","days","weeks","years"]})]),
                simple_aggregator("trim", "TRIM", "Trims off the first, last, or both data points.",
                    vec![json!({"name": "trim", "label": "Trim", "type": "enum", "default_value": "both",
                                "options": ["first","last","both"]})]),
            ]
        },
        {
            "name": "group_by",
            "label": "Group By",
            "properties": [
                simple_aggregator("tag", "Tag", "Groups data points by tag names.",
                    vec![json!({"name": "tags", "label": "Tags", "type": "array"})]),
                simple_aggregator("time", "Time", "Groups data points in time ranges.",
                    vec![json!({"name": "range_size", "label": "Range size", "type": "Object"}),
                         json!({"name": "group_count", "label": "Count", "type": "int"})]),
                simple_aggregator("value", "Value", "Groups data points by value.",
                    vec![json!({"name": "range_size", "label": "Range size", "type": "int"})]),
                simple_aggregator("bin", "Bin", "Groups data points into bins.",
                    vec![json!({"name": "bins", "label": "Bins", "type": "array"})]),
            ]
        }
    ])
}
