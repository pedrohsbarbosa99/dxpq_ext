#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <libpq-fe.h>
#include "connection.h"

void PGConnection_dealloc(PGConnection *self) {
    if (self->conn) {
        PQfinish(self->conn);
        self->conn = NULL;
    }
    PyObject_Del(self);
}


PyObject *PGConnection_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    PGConnection *self;
    self = (PGConnection *) type->tp_alloc(type, 0);
    if (self != NULL) {
        self->conn = NULL;
    }
    return (PyObject *) self;
}

int PGConnection_init(PGConnection *self, PyObject *args, PyObject *kwds) {
    const char *conninfo;
    char *listkws[] = {"conninfo", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", listkws, &conninfo)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to parse arguments");
        return -1;
    }

    self->conn = PQconnectdb(conninfo);
    if (PQstatus(self->conn) != CONNECTION_OK) {
        PyErr_SetString(PyExc_RuntimeError, PQerrorMessage(self->conn));
        return -1;
    }

    return 0;
}

PyObject *PGConnection_close(PGConnection *self) {
    if (self->conn) {
        PQfinish(self->conn);
        self->conn = NULL;
    }
    Py_RETURN_NONE;
}


static PyMethodDef PGConnection_methods[] = {
    {"close", (PyCFunction)PGConnection_close, METH_NOARGS, "Close the connection"},
    {NULL}
};

PyTypeObject PGConnectionType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "dxpq_ext.PGConnection",
    .tp_doc = "PostgreSQL Connection Object",
    .tp_basicsize = sizeof(PGConnection),
    .tp_itemsize = 0, 
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PGConnection_new,
    .tp_init = (initproc) PGConnection_init,
    .tp_dealloc = (destructor) PGConnection_dealloc,
    .tp_methods = PGConnection_methods,
};