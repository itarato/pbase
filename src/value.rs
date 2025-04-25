use std::cmp::Ordering;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Value {
    NULL,
    I32(i32),
    U8(u8),
}

impl Value {
    pub fn copy_bytes_to(&self, buf: &mut [u8]) {
        match self {
            Self::NULL => {} // Noop.
            Self::I32(v) => buf[0..4].copy_from_slice(&v.to_le_bytes()),
            Self::U8(v) => buf[0] = *v,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::NULL, Self::NULL) => Ordering::Equal,

            (Self::NULL, Self::I32(_) | Self::U8(_)) => Ordering::Less,
            (Self::I32(_) | Self::U8(_), Self::NULL) => Ordering::Greater,

            (Self::I32(lhs), Self::I32(rhs)) => lhs.cmp(rhs),
            (Self::U8(lhs), Self::U8(rhs)) => lhs.cmp(rhs),

            _ => panic!("Values cannot be compared {self:?} ? {other:?  }"),
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
        Value::I32(0x0403_0201).copy_bytes_to(&mut buf[1..]);

        assert_eq!(vec![0, 1, 2, 3, 4, 0], buf);
    }
}
