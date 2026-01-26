use pyo3::prelude::*;

/// ROAM Agent interface
#[pyclass]
struct Agent {
    id: String,
}

#[pymethods]
impl Agent {
    #[new]
    fn new(id: String) -> Self {
        Agent { id }
    }

    fn connect(&self) -> PyResult<String> {
        Ok(format!("Connected agent {}", self.id))
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn roam(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Agent>()?;
    Ok(())
}
