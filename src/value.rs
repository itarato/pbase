use std::cmp::Ordering;

#[derive(Debug, PartialEq, PartialOrd, Eq, Clone)]
pub enum Value {
    NULL,
    I32(i32),
    U8(u8),
}

impl Value {
    pub fn copy_bytes_to(&self, buf: &mut [u8]) {
        match self {
            Value::NULL => {} // Noop.
            Value::I32(v) => buf[0..4].copy_from_slice(&v.to_le_bytes()),
            Value::U8(v) => buf[0] = *v,
        };
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::NULL, Value::NULL) => Ordering::Equal,

            (Value::NULL, Value::I32(_)) => Ordering::Less,
            (Value::I32(_), Value::NULL) => Ordering::Greater,

            (Value::NULL, Value::U8(_)) => Ordering::Less,
            (Value::U8(_), Value::NULL) => Ordering::Greater,

            (Value::I32(lhs), Value::I32(rhs)) => lhs.cmp(rhs),
            (Value::U8(lhs), Value::U8(rhs)) => lhs.cmp(rhs),

            _ => panic!("Values cannot be compared {:?} ? {:?}", self, other),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_value_ordering() {
        let null = Value::NULL;
        let i32_zero = Value::I32(0);
        let i32_ten = Value::I32(10);

        assert_eq!(null, null);
        assert_eq!(i32_ten, i32_ten);

        assert!(null < i32_zero);
        assert!(null < i32_ten);

        assert!(null <= i32_zero);
        assert!(null <= i32_ten);

        assert!(i32_zero > null);
        assert!(i32_ten > null);

        assert!(i32_zero >= null);
        assert!(i32_ten >= null);

        assert!(i32_zero < i32_ten);
        assert!(i32_zero <= i32_ten);
    }

    #[test]
    fn test_copy_bytes_to() {
        let mut buf: [u8; 6] = [0; 6];
        Value::I32(0x04030201).copy_bytes_to(&mut buf[1..]);

        assert_eq!(vec![0, 1, 2, 3, 4, 0], buf);
    }
}
