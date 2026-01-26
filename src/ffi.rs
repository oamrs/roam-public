use std::ffi::{CStr, CString};
use std::os::raw::c_char;

/// C-ABI compatible interface for .NET/Go integration (FFI)
#[no_mangle]
pub extern "C" fn oam_version() -> *mut c_char {
    let version = env!("CARGO_PKG_VERSION");
    CString::new(version).unwrap().into_raw()
}

/// Frees a string allocated by this library.
///
/// # Safety
///
/// `s` must be a pointer previously granted by this library.
#[no_mangle]
pub unsafe extern "C" fn oam_free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe {
        let _ = CString::from_raw(s);
    }
}

/// Connects an agent using the native FFI interface.
///
/// # Safety
///
/// `agent_id` must be a valid, null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn oam_agent_connect(agent_id: *const c_char) -> *mut c_char {
    let c_str = unsafe {
        assert!(!agent_id.is_null());
        CStr::from_ptr(agent_id)
    };

    let r_str = c_str.to_str().unwrap_or("unknown");
    // In real world, this would call the async executor/interceptor logic
    let response = format!("Native: Agent {} connected via FFI", r_str);

    CString::new(response).unwrap().into_raw()
}
