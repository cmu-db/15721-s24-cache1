#[allow(dead_code)]

fn advanced_database() -> u32 {
    15721
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_advanced_database() {
        assert_eq!(advanced_database(), 15721);
    }
}
