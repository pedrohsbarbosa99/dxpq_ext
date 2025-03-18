#ifndef CURSOR_H
#define CURSOR_H

#include <Python.h>
#include <libpq-fe.h>
#include "connection.h"


typedef struct {
    PyObject_HEAD
    PGresult *result;
    PGConnection *PGConnection;
    char* cursor_type;
} PGCursor;

void PGCursor_dealloc(PGCursor *self);
PyObject *PGCursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds);
int PGCursor_init(PGCursor *self, PyObject *args, PyObject *kwds);
PyObject *PGCursor_execute(PGCursor *self, PyObject *args, PyObject *kwds);
PyObject *PGCursor_fetchone(PGCursor *self, PyObject *args);
PyObject *PGCursor_fetchall(PGCursor *self, PyObject *args);
PyObject *PGCursor_close(PGCursor *self);
extern PyTypeObject PGCursorType;

#endif
