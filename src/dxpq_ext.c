#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>
#include "connection.h"
#include "cursor.h"


static PyMethodDef dxpq_ext_functions[] = {
    {NULL, NULL, 0, NULL}
};


static PyModuleDef dxpq_ext_mod = {
    PyModuleDef_HEAD_INIT,
    "dxpq_ext",
    "Driver to connect to Postgresql",
    -1,
    dxpq_ext_functions
};

PyMODINIT_FUNC PyInit_dxpq_ext(void) {
    PyObject *m;

    if (PyType_Ready(&PGConnectionType) < 0)
        return NULL;
    
    if (PyType_Ready(&PGCursorType) < 0)
        return NULL;

    m = PyModule_Create(&dxpq_ext_mod);
    if (m == NULL)
        return NULL;

    Py_INCREF(&PGConnectionType);
    PyModule_AddObject(m, "PGConnection", (PyObject *)&PGConnectionType);

    Py_INCREF(&PGCursorType);
    PyModule_AddObject(m, "PGCursor", (PyObject *)&PGCursorType);

    return m;
}

