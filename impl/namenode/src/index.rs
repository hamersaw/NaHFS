use query::{BinaryExpression, BooleanExpression, BooleanOp, CompareExpression, CompareOp, ConstantExpression, EvaluateExpression};
use radix::{self, BooleanOperation, PrefixExpression, PrefixOperation, RadixQuery, RadixTrie};
use regex::Regex;
use shared::{self, AtlasError};

use std::collections::HashMap;

pub struct Index {
    trie: RadixTrie<Vec<(u64, u32)>>,
    timestamps: HashMap<u64, (u64, u64)>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            trie: RadixTrie::new(),
            timestamps: HashMap::new(),
        }
    }

    pub fn add_geohash(&mut self, geohash: &str,
            block_id: &u64, length: u32) -> Result<(), AtlasError> {
        let bytes = geohash.as_bytes();
        if let Some(blocks) = self.trie.get_mut(&bytes) {
            // check if block already exists
            for (value, _) in blocks.iter() {
                if block_id == value {
                    return Ok(());
                }
            }

            trace!("adding geohash index {} : ({}, {})",
                geohash, block_id, length);
            blocks.push((*block_id, length));
        } else {
            trace!("inserting new geohash index {} : ({}, {})",
                geohash, block_id, length);
            self.trie.insert(&bytes,
                vec!((*block_id, length)))?;
        }

        Ok(())
    }

    pub fn add_time_range(&mut self, start_timestamp: u64,
            end_timestamp: u64, block_id: &u64) -> Result<(), AtlasError> {
        // check if block already exists
        if !self.timestamps.contains_key(block_id) {
            trace!("inserting new timestamp index {} : ({}, {})",
                block_id, start_timestamp, end_timestamp);
            self.timestamps.insert(*block_id,
                (start_timestamp, end_timestamp));
        }

        Ok(())
    }

    pub fn query(&self, boolean_expr: &BooleanExpression<u64>,
            radix_query: &RadixQuery, block_ids: &Vec<u64>)
            -> HashMap<u64, (Vec<u8>, Vec<u32>)> {
        // initialize geohash map
        let mut geohash_map = HashMap::new();
        for block_id in block_ids {
            // retrieve timestamp index for block
            let block_result = self.timestamps.get(block_id);
            if let None = block_result {
                warn!("unable to find timestamp index for block '{}'",
                    block_id);
                continue;
            }

            let (start, end) = block_result.unwrap();

            // if valid timestamps -> process with geohash query
            if boolean_expr.operand_count() == 0
                    || boolean_expr.evaluate_bin(start, end) {
                geohash_map.insert(*block_id, (Vec::new(), Vec::new()));
            }
        }

        // evaluate query
        radix_query.evaluate(&self.trie, 
            &mut geohash_map, &mut query_process);

        // return geohash map
        geohash_map
    }
}

fn query_process(key: &Vec<u8>, value: &Vec<(u64, u32)>,
        token: &mut HashMap<u64, (Vec<u8>, Vec<u32>)>) {
    for (block_id, length) in value {
        if let Some((geohashes, lengths)) = token.get_mut(&block_id) {
            // if token contains block id -> add geohash
            let c = key[key.len() - 1] as char;
            let geohash_key = match shared::geohash_char_to_value(c) {
                Ok(geohash_key) => geohash_key,
                Err(e) => {
                    warn!("failed to parse geohash: {}", e);
                    continue;
                },
            };

            geohashes.push(geohash_key);
            lengths.push(*length);
        }
    }
}

/*pub fn parse_query(query_string: &str) -> Result<RadixQuery, AtlasError> {
    Ok(radix::parse_query(query_string)?)
}*/

pub fn parse_query(query_string: &str)
        -> Result<(BooleanExpression<u64>, RadixQuery), AtlasError> {
    // test if query is valid
    let regex = Regex::new(r"^(\w+(=|!=|<|<=|>|>=)\w+)?(&\s*\w+(=|!=|<|<=|>|>=)\w+)?$")?;
    if !regex.is_match(query_string) {
        return Err(AtlasError::from("misformatted input string"));
    }

    // parse query expressions
    let mut geohash_expressions = Vec::new();
    let mut timestamp_expressions: Vec<Box<BinaryExpression<u64>>>
        = Vec::new();

    let expr_regex = Regex::new(r"(\w+)(=|!=|<|<=|>|>=)(\w+)")?;
    for expr in expr_regex.captures_iter(query_string) {
        match &expr[1] {
            "geohash" | "g" => {
                // parse geohash expression
                let geohash_expression = match &expr[2] {
                    "=" => PrefixExpression::new(expr[3].to_string(),
                        PrefixOperation::Equal),
                    "!=" => PrefixExpression::new(expr[3].to_string(),
                        PrefixOperation::NotEqual),
                    _ => return Err(AtlasError::from(
                        format!("'{}' unsuported on geohashes", &expr[2]))),
                };

                geohash_expressions.push(geohash_expression);
            },
            "timestamp" | "t" => {
                // parse timestamp expression
                let evaluate_expr =
                    Box::new(EvaluateExpression::<u64>::new());
                let constant_expr = Box::new(ConstantExpression::<u64>
                    ::new((&expr[3]).parse::<u64>()?));

                let timestamp_expression = match &expr[2] {
                    "<" => CompareExpression::<u64>::new(evaluate_expr,
                        constant_expr, CompareOp::LessThan),
                    "<=" => CompareExpression::<u64>::new(evaluate_expr,
                        constant_expr, CompareOp::LessThanOrEqualTo),
                    ">" => CompareExpression::<u64>::new(evaluate_expr,
                        constant_expr, CompareOp::GreaterThan),
                    ">=" => CompareExpression::<u64>::new(evaluate_expr,
                        constant_expr, CompareOp::GreaterThanOrEqualTo),
                    _ => return Err(AtlasError::from(
                        format!("'{}' unsuported on timestamp", &expr[2]))),
                };

                timestamp_expressions.push(
                    Box::new(timestamp_expression));
            },
            _ => return Err(AtlasError::from(
                format!("unsupported variable: '{}'", &expr[1]))),
        }
    }

    // return timestamp and geohash queries
    Ok((BooleanExpression::new(timestamp_expressions, BooleanOp::And),
        RadixQuery::new(geohash_expressions, BooleanOperation::And)))
}
