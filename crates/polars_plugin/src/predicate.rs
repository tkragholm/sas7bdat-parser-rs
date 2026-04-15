#[cfg(feature = "arrow")]
use super::Dataset;
#[cfg(feature = "arrow")]
use polars::frame::DataFrame;
#[cfg(feature = "arrow")]
use polars::prelude::{
    AnyValue, BooleanChunked, ChunkCompareEq, ChunkCompareIneq, ChunkFull, Column,
    DataType as PlDataType, PlSmallStr, Scalar,
};
#[cfg(feature = "arrow")]
use polars_plan::prelude::{
    BooleanFunction, DataTypeExpr, Expr, FunctionExpr, LiteralValue, Operator,
};
#[cfg(feature = "arrow")]
use pyo3::prelude::*;
#[cfg(feature = "arrow")]
use rmp_serde::from_slice;
#[cfg(feature = "arrow")]
use sas7bdat_simd::{Error, Result as SasResult};

#[cfg(feature = "arrow")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    EqValidity,
    Ne,
    NeValidity,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone)]
pub enum PredicateExpr {
    Const(bool),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
    Compare {
        left: PredicateOperand,
        op: CompareOp,
        right: PredicateOperand,
    },
    IsNull(PredicateOperand),
    IsNotNull(PredicateOperand),
    IsFinite(PredicateOperand),
    IsInfinite(PredicateOperand),
    IsNan(PredicateOperand),
    IsNotNan(PredicateOperand),
}

#[cfg(feature = "arrow")]
#[derive(Debug, Clone)]
pub enum PredicateOperand {
    Column {
        name: String,
        cast: Option<PlDataType>,
    },
    Scalar {
        value: Scalar,
        cast: Option<PlDataType>,
    },
}

#[cfg(feature = "arrow")]
pub fn prepare_predicate(
    py: Python<'_>,
    ds: &Dataset,
    predicate: Option<Py<PyAny>>,
) -> (Option<PredicateExpr>, Option<Py<PyAny>>) {
    let Some(predicate) = predicate else {
        return (None, None);
    };

    predicate_from_python(py, ds, &predicate).map_or_else(
        || (None, Some(predicate)),
        |predicate| (Some(predicate), None),
    )
}

#[cfg(feature = "arrow")]
pub fn predicate_from_python(
    py: Python<'_>,
    ds: &Dataset,
    predicate: &Py<PyAny>,
) -> Option<PredicateExpr> {
    let predicate = predicate.bind(py);
    let meta = predicate.getattr("meta").ok()?;
    let serialized = meta.call_method0("serialize").ok()?;
    let serialized: Vec<u8> = serialized.extract().ok()?;
    let expr: Expr = from_slice(&serialized).ok()?;
    parse_predicate_expr(ds, &expr)
}

#[cfg(feature = "arrow")]
pub fn append_unique_columns(base: &mut Vec<String>, extra: &[String]) {
    for column in extra {
        if !base.iter().any(|existing| existing == column) {
            base.push(column.clone());
        }
    }
}

#[cfg(feature = "arrow")]
pub fn parse_predicate_expr(ds: &Dataset, expr: &Expr) -> Option<PredicateExpr> {
    use BooleanFunction as B;
    use Operator as O;

    match expr {
        Expr::Literal(literal) => predicate_const(literal),
        Expr::Alias(inner, _) | Expr::KeepName(inner) | Expr::RenameAlias { expr: inner, .. } => {
            parse_predicate_expr(ds, inner)
        }
        Expr::BinaryExpr { left, op, right } if op.is_comparison() => {
            Some(PredicateExpr::Compare {
                left: parse_predicate_operand(ds, left.as_ref())?,
                op: compare_op(*op)?,
                right: parse_predicate_operand(ds, right.as_ref())?,
            })
        }
        Expr::BinaryExpr {
            left,
            op: O::And | O::LogicalAnd,
            right,
        } => Some(PredicateExpr::And(
            Box::new(parse_predicate_expr(ds, left.as_ref())?),
            Box::new(parse_predicate_expr(ds, right.as_ref())?),
        )),
        Expr::BinaryExpr {
            left,
            op: O::Or | O::LogicalOr,
            right,
        } => Some(PredicateExpr::Or(
            Box::new(parse_predicate_expr(ds, left.as_ref())?),
            Box::new(parse_predicate_expr(ds, right.as_ref())?),
        )),
        Expr::Function { input, function } if input.len() == 1 => match function {
            FunctionExpr::Boolean(B::Not) => Some(PredicateExpr::Not(Box::new(
                parse_predicate_expr(ds, &input[0])?,
            ))),
            FunctionExpr::Boolean(B::IsNull) => Some(PredicateExpr::IsNull(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsNotNull) => Some(PredicateExpr::IsNotNull(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsFinite) => Some(PredicateExpr::IsFinite(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsInfinite) => Some(PredicateExpr::IsInfinite(
                parse_predicate_operand(ds, &input[0])?,
            )),
            FunctionExpr::Boolean(B::IsNan) => Some(PredicateExpr::IsNan(parse_predicate_operand(
                ds, &input[0],
            )?)),
            FunctionExpr::Boolean(B::IsNotNan) => Some(PredicateExpr::IsNotNan(
                parse_predicate_operand(ds, &input[0])?,
            )),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(feature = "arrow")]
pub fn parse_predicate_operand(ds: &Dataset, expr: &Expr) -> Option<PredicateOperand> {
    match expr {
        Expr::Column(name) => {
            ds.column(name.as_str())?;
            Some(PredicateOperand::Column {
                name: name.to_string(),
                cast: None,
            })
        }
        Expr::Literal(literal) => {
            literal_to_scalar(literal).map(|value| PredicateOperand::Scalar { value, cast: None })
        }
        Expr::Alias(inner, _) | Expr::KeepName(inner) | Expr::RenameAlias { expr: inner, .. } => {
            parse_predicate_operand(ds, inner)
        }
        Expr::Cast {
            expr: inner, dtype, ..
        } => {
            let cast = match dtype {
                DataTypeExpr::Literal(dtype) => Some(dtype.clone()),
                _ => None,
            }?;
            let mut operand = parse_predicate_operand(ds, inner.as_ref())?;
            operand.set_cast(cast);
            Some(operand)
        }
        _ => None,
    }
}

#[cfg(feature = "arrow")]
fn predicate_const(literal: &LiteralValue) -> Option<PredicateExpr> {
    let LiteralValue::Scalar(scalar) = literal.clone().materialize() else {
        return None;
    };
    match scalar.as_any_value() {
        AnyValue::Boolean(value) => Some(PredicateExpr::Const(value)),
        _ => None,
    }
}

#[cfg(feature = "arrow")]
fn literal_to_scalar(literal: &LiteralValue) -> Option<Scalar> {
    match literal.clone().materialize() {
        LiteralValue::Scalar(scalar) => Some(scalar),
        LiteralValue::Series(series) if series.len() == 1 => {
            let value = series.get(0).ok()?;
            Some(Scalar::new(value.dtype(), value.into_static()))
        }
        _ => None,
    }
}

#[cfg(feature = "arrow")]
const fn compare_op(op: Operator) -> Option<CompareOp> {
    Some(match op {
        Operator::Eq => CompareOp::Eq,
        Operator::EqValidity => CompareOp::EqValidity,
        Operator::NotEq => CompareOp::Ne,
        Operator::NotEqValidity => CompareOp::NeValidity,
        Operator::Gt => CompareOp::Gt,
        Operator::GtEq => CompareOp::GtEq,
        Operator::Lt => CompareOp::Lt,
        Operator::LtEq => CompareOp::LtEq,
        _ => return None,
    })
}

#[cfg(feature = "arrow")]
impl PredicateExpr {
    pub(super) fn collect_columns(&self, columns: &mut Vec<String>) {
        match self {
            Self::Const(_) => (),
            Self::And(left, right) | Self::Or(left, right) => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Self::Not(inner) => inner.collect_columns(columns),
            Self::Compare { left, right, .. } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            Self::IsNull(operand)
            | Self::IsNotNull(operand)
            | Self::IsFinite(operand)
            | Self::IsInfinite(operand)
            | Self::IsNan(operand)
            | Self::IsNotNan(operand) => operand.collect_columns(columns),
        }
    }
}

#[cfg(feature = "arrow")]
impl PredicateOperand {
    fn collect_columns(&self, columns: &mut Vec<String>) {
        if let Self::Column { name, .. } = self
            && !columns.iter().any(|existing| existing == name)
        {
            columns.push(name.clone());
        }
    }

    fn set_cast(&mut self, cast: PlDataType) {
        match self {
            Self::Column { cast: slot, .. } | Self::Scalar { cast: slot, .. } => {
                *slot = Some(cast);
            }
        }
    }
}

#[cfg(feature = "arrow")]
pub fn filter_dataframe(df: &DataFrame, predicate: &PredicateExpr) -> SasResult<DataFrame> {
    let mask = evaluate_predicate(df, predicate)?;
    df.filter(&mask).map_err(|err| Error::io(err.to_string()))
}

#[cfg(feature = "arrow")]
pub fn evaluate_predicate(df: &DataFrame, predicate: &PredicateExpr) -> SasResult<BooleanChunked> {
    match predicate {
        PredicateExpr::Const(value) => {
            Ok(BooleanChunked::full(PlSmallStr::EMPTY, *value, df.height()))
        }
        PredicateExpr::And(left, right) => {
            let left = evaluate_predicate(df, left)?;
            let right = evaluate_predicate(df, right)?;
            Ok(&left & &right)
        }
        PredicateExpr::Or(left, right) => {
            let left = evaluate_predicate(df, left)?;
            let right = evaluate_predicate(df, right)?;
            Ok(&left | &right)
        }
        PredicateExpr::Not(inner) => Ok(!evaluate_predicate(df, inner)?),
        PredicateExpr::Compare { left, op, right } => {
            let left = resolve_operand(df, left)?;
            let right = resolve_operand(df, right)?;
            let mask = match op {
                CompareOp::Eq => left.equal(&right),
                CompareOp::EqValidity => left.equal_missing(&right),
                CompareOp::Ne => left.not_equal(&right),
                CompareOp::NeValidity => left.not_equal_missing(&right),
                CompareOp::Gt => left.gt(&right),
                CompareOp::GtEq => left.gt_eq(&right),
                CompareOp::Lt => left.lt(&right),
                CompareOp::LtEq => left.lt_eq(&right),
            }
            .map_err(|err| Error::io(err.to_string()))?;
            Ok(mask)
        }
        PredicateExpr::IsNull(operand) => Ok(resolve_operand(df, operand)?.is_null()),
        PredicateExpr::IsNotNull(operand) => Ok(resolve_operand(df, operand)?.is_not_null()),
        PredicateExpr::IsFinite(operand) => resolve_operand(df, operand)?
            .is_finite()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsInfinite(operand) => resolve_operand(df, operand)?
            .is_infinite()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsNan(operand) => resolve_operand(df, operand)?
            .is_nan()
            .map_err(|err| Error::io(err.to_string())),
        PredicateExpr::IsNotNan(operand) => Ok(!resolve_operand(df, operand)?
            .is_nan()
            .map_err(|err| Error::io(err.to_string()))?),
    }
}

#[cfg(feature = "arrow")]
fn resolve_operand(df: &DataFrame, operand: &PredicateOperand) -> SasResult<Column> {
    match operand {
        PredicateOperand::Column { name, cast } => {
            let mut column = df
                .column(name)
                .map_err(|err| Error::io(err.to_string()))?
                .clone();
            if let Some(dtype) = cast {
                column = column
                    .cast(dtype)
                    .map_err(|err| Error::io(err.to_string()))?;
            }
            Ok(column)
        }
        PredicateOperand::Scalar { value, cast } => {
            let mut column = Column::new_scalar(PlSmallStr::EMPTY, value.clone(), df.height());
            if let Some(dtype) = cast {
                column = column
                    .cast(dtype)
                    .map_err(|err| Error::io(err.to_string()))?;
            }
            Ok(column)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::df;

    #[test]
    fn append_unique_columns_keeps_order_and_deduplicates() {
        let mut base = vec!["a".to_string(), "b".to_string()];
        append_unique_columns(
            &mut base,
            &["b".to_string(), "c".to_string(), "a".to_string()],
        );
        assert_eq!(base, vec!["a", "b", "c"]);
    }

    #[test]
    fn filter_dataframe_handles_is_not_null() {
        let df = df!(
            "x" => [1i32, 2, 3],
            "y" => [Some(10i32), None, Some(30)]
        )
        .expect("dataframe");
        let predicate = PredicateExpr::IsNotNull(PredicateOperand::Column {
            name: "y".to_string(),
            cast: None,
        });

        let filtered = filter_dataframe(&df, &predicate).expect("filtered dataframe");
        assert_eq!(filtered.height(), 2);
        let values: Vec<i32> = filtered
            .column("x")
            .expect("x column")
            .i32()
            .expect("x as i32")
            .into_no_null_iter()
            .collect();
        assert_eq!(values, vec![1, 3]);
    }
}
