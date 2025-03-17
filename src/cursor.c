#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <libpq-fe.h>
#include "cursor.h"

void PGCursor_dealloc(PGCursor *self) {
    if (self->result) {
        PQclear(self->result);
    }
    PyObject_Del(self);
}


PyObject *PGCursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
    PGCursor *self;
    self = (PGCursor *) type->tp_alloc(type, 0);
    if (self != NULL) {
        self->result = NULL;
        self->PGConnection = NULL;
    }
    return (PyObject *) self;
}

int PGCursor_init(PGCursor *self, PyObject *args, PyObject *kwds) {
    const PyObject *connection;
    char *listkws[] = {"connection", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", listkws, &connection)) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to parse arguments");
        return -1;
    }
    self->PGConnection = (PGConnection *)connection;
    return 0;
}


PyObject *PGCursor_execute_params(PGCursor *self, PyObject *args, PyObject *kwds) {
    const char *sql;
    PyObject *params = NULL;
    char *listkws[] = {"sql", "params", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "sO", listkws, &sql, &params)) {
        if (!sql) {
            PyErr_SetString(PyExc_RuntimeError, "argument 'sql' is required");
            return NULL;
        }
        if (!params) {
            PyErr_SetString(PyExc_RuntimeError, "argument 'params' is required");
            return NULL;
        }
        return NULL;
    }

    int nParams = (int)PyTuple_Size(params);
    const Oid *paramTypes = NULL;


    const char **paramValues = malloc(nParams * sizeof(char *));

    int *paramLengths = malloc(nParams * sizeof(int));
    int *paramFormats = malloc(nParams * sizeof(int));

    if (!paramValues || !paramLengths || !paramFormats) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to allocate memory");
        free(paramValues);
        free(paramLengths);
        free(paramFormats);
        return NULL;
    }

    for (int i = 0; i < nParams; i++) {
        PyObject *item = PyTuple_GetItem(params, i);
        
        if (!item) {
            PyErr_SetString(PyExc_IndexError, "Erro ao acessar o item da tupla");
            free(paramValues);
            free(paramLengths);
            free(paramFormats);
            return NULL;
        }

        PyObject *str_item = PyObject_Str(item);
        
        if (str_item) {
            if (str_item != NULL) {
                const char* str;
                if (PyBool_Check(item)) {
                    str = PyObject_IsTrue(item) == 1 ? "TRUE" : "FALSE";
                } else {
                    str = PyUnicode_AsUTF8(str_item);
                }

                paramLengths[i] = sizeof(item);
                paramFormats[i] = 0;
                paramValues[i] = strdup(str);
                
            }
            Py_DECREF(str_item);
        } else {
            PyErr_SetString(PyExc_TypeError, "Falha ao converter o item para string");
            free(paramValues);
            free(paramLengths);
            free(paramFormats);
            return NULL;
        }

    }

    PGresult *result = PQexecParams(
        self->PGConnection->conn,
        sql,
        nParams,
        paramTypes,
        paramValues,
        paramLengths,
        paramFormats,
        0
    );

    free(paramLengths);
    free(paramFormats);

    for (int i = 0; i < nParams; i++) {
        free((char *)paramValues[i]);
    }
    
    free(paramValues);

    if (PQresultStatus(result) != PGRES_COMMAND_OK && PQresultStatus(result) != PGRES_TUPLES_OK) {
        PyErr_SetString(PyExc_RuntimeError, PQresultErrorMessage(result));
        return NULL;
    }
    
    self->result = result;

    Py_RETURN_NONE;
    
}


PyObject *PGCursor_execute(PGCursor *self, PyObject *args, PyObject *kwds) {
    const char *sql;
    char *listkws[] = {"sql", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", listkws, &sql)) {
        PyErr_SetString(PyExc_RuntimeError, "argument 'sql' is required");
        return NULL;
    }
    PGresult *result = PQexec(self->PGConnection->conn, sql);
    if (PQresultStatus(result) != PGRES_COMMAND_OK && PQresultStatus(result) != PGRES_TUPLES_OK) {
        PyErr_SetString(PyExc_RuntimeError, PQresultErrorMessage(result));
        return NULL;
    }
    
    self->result = result;

    Py_RETURN_NONE;
    
}


PyObject *PGCursor_fetchall(PGCursor *self, PyObject *args) {
    if (!self->result) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor has no result");
        return NULL;
    }

    PGresult *result = self->result;

    int nrows = PQntuples(result);
    int ncols = PQnfields(result);

    PyObject *list_results = PyList_New(nrows);

    for (int i = 0; i < nrows; i++) {
        PyObject *row = PyTuple_New(ncols);

        for (int j = 0; j < ncols; j++) {
            PyObject *value = PyUnicode_FromString(PQgetvalue(result, i, j));
            PyTuple_SET_ITEM(row, j, value);
        }

        PyList_SET_ITEM(list_results, i, row);
    }

    self->result = NULL;
    return list_results;
}


PyObject *PGCursor_fetchone(PGCursor *self, PyObject *args) {
    if (!self->result) {
        PyErr_SetString(PyExc_RuntimeError, "Cursor has no result");
        return NULL;
    }

    PGresult *result = self->result;

    int nrows = PQntuples(result);
    int ncols = PQnfields(result);

    if (nrows == 0) {
        Py_RETURN_NONE;
    }

    PyObject *row = PyTuple_New(ncols);

    for (int j = 0; j < ncols; j++) {
        PyObject *value = PyUnicode_FromString(PQgetvalue(result, 0, j));
        PyTuple_SET_ITEM(row, j, value);
    }

    self->result = NULL;
    return row;
}

PyObject *PGCursor_close(PGCursor *self) {
    if (self->PGConnection && self->result) {
        PQclear(self->result);
        self->result = NULL;
    }
    Py_RETURN_NONE;
}


PyMethodDef PGCursor_methods[] = {
    {"execute", (PyCFunction)PGCursor_execute, METH_VARARGS | METH_KEYWORDS, "Execute a query"},
    {"execute_params", (PyCFunction)PGCursor_execute_params, METH_VARARGS | METH_KEYWORDS, "Execute a query with params"},
    {"fetchall", (PyCFunction)PGCursor_fetchall, METH_NOARGS, "Fetch all rows from a query"},
    {"fetchone", (PyCFunction)PGCursor_fetchone, METH_NOARGS, "Fetch one row from a query"},
    {"close", (PyCFunction)PGCursor_close, METH_NOARGS, "Close the cursor"},
    {NULL}
};

PyTypeObject PGCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "dxpq_ext.PGCursor",
    .tp_doc = "PostgreSQL Cursor Object",
    .tp_basicsize = sizeof(PGCursor),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PGCursor_new,
    .tp_init = (initproc) PGCursor_init,
    .tp_dealloc = (destructor) PGCursor_dealloc,
    .tp_methods = PGCursor_methods,
};