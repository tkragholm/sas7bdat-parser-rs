
#include <stdint.h>
#include <Rinternals.h>
#include <R_ext/Parse.h>

#include "rust/api.h"

static uintptr_t TAGGED_POINTER_MASK = (uintptr_t)1;

SEXP handle_result(SEXP res_) {
    uintptr_t res = (uintptr_t)res_;

    // An error is indicated by tag.
    if ((res & TAGGED_POINTER_MASK) == 1) {
        // Remove tag
        SEXP res_aligned = (SEXP)(res & ~TAGGED_POINTER_MASK);

        // Currently, there are two types of error cases:
        //
        //   1. Error from Rust code
        //   2. Error from R's C API, which is caught by R_UnwindProtect()
        //
        if (TYPEOF(res_aligned) == CHARSXP) {
            // In case 1, the result is an error message that can be passed to
            // Rf_errorcall() directly.
            Rf_errorcall(R_NilValue, "%s", CHAR(res_aligned));
        } else {
            // In case 2, the result is the token to restart the
            // cleanup process on R's side.
            R_ContinueUnwind(res_aligned);
        }
    }

    return (SEXP)res;
}

SEXP savvy_hello__impl(void) {
    SEXP res = savvy_hello__ffi();
    return handle_result(res);
}

SEXP savvy_int_times_int__impl(SEXP c_arg__x, SEXP c_arg__y) {
    SEXP res = savvy_int_times_int__ffi(c_arg__x, c_arg__y);
    return handle_result(res);
}

SEXP savvy_read_sas__impl(SEXP c_arg__path) {
    SEXP res = savvy_read_sas__ffi(c_arg__path);
    return handle_result(res);
}

SEXP savvy_sas_column_names__impl(SEXP c_arg__path) {
    SEXP res = savvy_sas_column_names__ffi(c_arg__path);
    return handle_result(res);
}

SEXP savvy_sas_metadata_json__impl(SEXP c_arg__path) {
    SEXP res = savvy_sas_metadata_json__ffi(c_arg__path);
    return handle_result(res);
}

SEXP savvy_sas_row_count__impl(SEXP c_arg__path) {
    SEXP res = savvy_sas_row_count__ffi(c_arg__path);
    return handle_result(res);
}

SEXP savvy_to_upper__impl(SEXP c_arg__x) {
    SEXP res = savvy_to_upper__ffi(c_arg__x);
    return handle_result(res);
}

SEXP savvy_write_sas__impl(SEXP c_arg__path, SEXP c_arg__sink, SEXP c_arg__output) {
    SEXP res = savvy_write_sas__ffi(c_arg__path, c_arg__sink, c_arg__output);
    return handle_result(res);
}

SEXP savvy_Person_associated_function__impl(void) {
    SEXP res = savvy_Person_associated_function__ffi();
    return handle_result(res);
}

SEXP savvy_Person_name__impl(SEXP self__) {
    SEXP res = savvy_Person_name__ffi(self__);
    return handle_result(res);
}

SEXP savvy_Person_new__impl(void) {
    SEXP res = savvy_Person_new__ffi();
    return handle_result(res);
}

SEXP savvy_Person_set_name__impl(SEXP self__, SEXP c_arg__name) {
    SEXP res = savvy_Person_set_name__ffi(self__, c_arg__name);
    return handle_result(res);
}


static const R_CallMethodDef CallEntries[] = {
    {"savvy_hello__impl", (DL_FUNC) &savvy_hello__impl, 0},
    {"savvy_int_times_int__impl", (DL_FUNC) &savvy_int_times_int__impl, 2},
    {"savvy_read_sas__impl", (DL_FUNC) &savvy_read_sas__impl, 1},
    {"savvy_sas_column_names__impl", (DL_FUNC) &savvy_sas_column_names__impl, 1},
    {"savvy_sas_metadata_json__impl", (DL_FUNC) &savvy_sas_metadata_json__impl, 1},
    {"savvy_sas_row_count__impl", (DL_FUNC) &savvy_sas_row_count__impl, 1},
    {"savvy_to_upper__impl", (DL_FUNC) &savvy_to_upper__impl, 1},
    {"savvy_write_sas__impl", (DL_FUNC) &savvy_write_sas__impl, 3},
    {"savvy_Person_associated_function__impl", (DL_FUNC) &savvy_Person_associated_function__impl, 0},
    {"savvy_Person_name__impl", (DL_FUNC) &savvy_Person_name__impl, 1},
    {"savvy_Person_new__impl", (DL_FUNC) &savvy_Person_new__impl, 0},
    {"savvy_Person_set_name__impl", (DL_FUNC) &savvy_Person_set_name__impl, 2},
    {NULL, NULL, 0}
};

void R_init_SASreaderRUST(DllInfo *dll) {
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);

    // Functions for initialzation, if any.

}
