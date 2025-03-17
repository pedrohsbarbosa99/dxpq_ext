#ifndef CONNECTION_H
#define CONNECTION_H

#include <Python.h>
#include <libpq-fe.h>

typedef struct {
    PyObject_HEAD
    PGconn *conn;
} PGConnection;

void PGConnection_dealloc(PGConnection *self);
PyObject *PGConnection_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int PGConnection_init(PGConnection *self, PyObject *args, PyObject *kwds);
PyObject *PGConnection_close(PGConnection *self);
extern PyTypeObject PGConnectionType;

#endif
