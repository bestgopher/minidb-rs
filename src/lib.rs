mod entry;
mod file;
mod method;

pub mod db;
pub mod error;

pub use crate::db::MiniDB;
pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
