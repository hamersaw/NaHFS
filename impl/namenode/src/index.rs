use prefix_query::{self, PrefixExpression, PrefixOperation};
use query::{self, BinaryExpression, CompareExpression, CompareOp, ConstantExpression, EvaluateExpression};
use regex::Regex;
use shared::{self, AtlasError};

use std::collections::{BTreeMap, HashMap};

pub type TemporalQuery = query::BooleanExpression<u64>;
pub type SpatialQuery = prefix_query::BooleanExpression;

pub struct Index {
    spatial_map: HashMap<u64, Vec<(String, u32)>>,
    temporal_map: HashMap<u64, (u64, u64)>,
}

impl Index {
    pub fn new() -> Index {
        Index {
            spatial_map: HashMap::new(),
            temporal_map: HashMap::new(),
        }
    }

    pub fn add_spatial_index(&mut self, block_id: u64, geohash: &str,
            length: u32) -> Result<(), AtlasError> {
        // add block entry in spatial map 
        let geohashes = self.spatial_map.entry(block_id)
            .or_insert(Vec::new());

        // check if geohash has already been indexed
        for (value, _) in geohashes.iter() {
            if value == geohash {
                return Ok(())
            }
        }

        // insert geohash index
        geohashes.push((geohash.to_string(), length));
        trace!("addeed spatial index on block {} : {} - {} bytes",
            block_id, geohash, length);

        Ok(())
    }

    pub fn add_temporal_index(&mut self, block_id: u64, 
            start_timestamp: u64, end_timestamp: u64)
            -> Result<(), AtlasError> {
        // check if block already exists
        if !self.temporal_map.contains_key(&block_id) {
            trace!("inserting new timestamp index {} : ({}, {})",
                block_id, start_timestamp, end_timestamp);
            self.temporal_map.insert(block_id,
                (start_timestamp, end_timestamp));
        }

        Ok(())
    }

    pub fn get_spatial_index(&self)
            -> &HashMap<u64, Vec<(String, u32)>> {
        &self.spatial_map
    }

    pub fn get_temporal_index(&self)
            -> &HashMap<u64, (u64, u64)> {
        &self.temporal_map
    }

    // returns
    //  Some(spatial entries) -> include spatial entries
    //  Some(empty) -> spatial index does not exist
    //  None -> block shouldn't be included
    pub fn spatial_query(&self, block_id: &u64,
            query: &SpatialQuery) -> Option<BTreeMap<u8, u32>> {
        let mut map = BTreeMap::new();
        match self.spatial_map.get(block_id) {
            Some(geohashes) => {
                // iterate over block spatial index
                for (geohash, length) in geohashes.iter() {
                    // if geohash evalutes true -> add information
                    if query.evaluate(geohash) {
                        let c = geohash.chars().rev().next().unwrap();

                        // convert char to geohash key
                        let geohash_key = match shared
                                ::geohash_char_to_value(c) {
                            Ok(geohash_key) => geohash_key,
                            Err(e) => {
                                warn!("failed to parse geohash: {}", e);
                                continue;
                            },
                        };

                        map.insert(geohash_key, *length);
                    }
                }

                // if map is empty -> don't include block
                match map.len() {
                    0 => None,
                    _ => Some(map),
                }
            },
            None => Some(map),
        }
    }

    pub fn temporal_query(&self, block_id: &u64,
            query: &TemporalQuery) -> bool {
        match self.temporal_map.get(block_id) {
            Some((start_timestamp, end_timestamp)) => 
                query.evaluate_bin(start_timestamp, end_timestamp),
            None => true,
        }
    }
}

pub fn parse_query(query_string: &str) -> Result<(Option<SpatialQuery>,
        Option<TemporalQuery>), AtlasError> {
    // test if query is valid
    let regex = Regex::new(r"^(\w+(=|!=|<|<=|>|>=)\w+)?(&\s*\w+(=|!=|<|<=|>|>=)\w+)?$")?;
    if !regex.is_match(query_string) {
        return Err(AtlasError::from("misformatted input string"));
    }

    // parse query expressions
    let mut spatial_expressions = Vec::new();
    let mut temporal_expressions: Vec<Box<BinaryExpression<u64>>>
        = Vec::new();

    let expr_regex = Regex::new(r"(\w+)(=|!=|<|<=|>|>=)(\w+)")?;
    for expr in expr_regex.captures_iter(query_string) {
        match &expr[1] {
            "geohash" | "g" => {
                // parse spatial expression
                let spatial_expression = match &expr[2] {
                    "=" => PrefixExpression::new(expr[3].to_string(),
                        PrefixOperation::Equal),
                    "!=" => PrefixExpression::new(expr[3].to_string(),
                        PrefixOperation::NotEqual),
                    _ => return Err(AtlasError::from(
                        format!("'{}' unsuported on geohashes", &expr[2]))),
                };

                spatial_expressions.push(spatial_expression);
            },
            "timestamp" | "t" => {
                // parse temporal expression
                let evaluate_expr =
                    Box::new(EvaluateExpression::<u64>::new());
                let constant_expr = Box::new(ConstantExpression::<u64>
                    ::new((&expr[3]).parse::<u64>()?));

                let temporal_expression = match &expr[2] {
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

                temporal_expressions.push(Box::new(temporal_expression));
            },
            _ => return Err(AtlasError::from(
                format!("unsupported variable: '{}'", &expr[1]))),
        }
    }

    // return spatial and temporal queries
    let spatial_query = match spatial_expressions.len() {
        0 => None,
        _ => Some(prefix_query::BooleanExpression::new(
            spatial_expressions, prefix_query::BooleanOperation::And)),
    };

    let temporal_query = match temporal_expressions.len() {
        0 => None,
        _ => Some(query::BooleanExpression
            ::new(temporal_expressions, query::BooleanOp::And)),
    };

    Ok((spatial_query, temporal_query))
}
