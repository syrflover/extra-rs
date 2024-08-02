pub trait MapInto<U> {
    type Target;

    fn map_into(self) -> Self::Target;
}

impl<T, U> MapInto<U> for Option<T>
where
    U: From<T>,
{
    type Target = Option<U>;

    fn map_into(self) -> Self::Target {
        self.map(Into::into)
    }
}

impl<T, E, U> MapInto<U> for Result<T, E>
where
    U: From<T>,
{
    type Target = Result<U, E>;

    fn map_into(self) -> Self::Target {
        self.map(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use crate::MapInto;

    #[test]
    fn map_into() {
        let x = Some("str");
        let _y: String = x.map_into().unwrap();
    }
}
