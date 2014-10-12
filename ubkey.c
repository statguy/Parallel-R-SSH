/*
gcc -shared -fPIC -I/usr/include/python2.7/ -lpython2.7 -o ubkey.so ubkey.c
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <termios.h>
#include <Python.h>

/* taken from: http://stackoverflow.com/a/2985246/3959140 */
static PyObject *getch(PyObject *self, PyObject *args) {
  int character;
  struct termios orig_term_attr;
  struct termios new_term_attr;
  
  /* set the terminal to raw mode */
  tcgetattr(fileno(stdin), &orig_term_attr);
  memcpy(&new_term_attr, &orig_term_attr, sizeof(struct termios));
  new_term_attr.c_lflag &= ~(ECHO|ICANON);
  new_term_attr.c_cc[VTIME] = 0;
  new_term_attr.c_cc[VMIN] = 0;
  tcsetattr(fileno(stdin), TCSANOW, &new_term_attr);
  
  /* read a character from the stdin stream without blocking */
  /*   returns EOF (-1) if no character is available */
  character = fgetc(stdin);
  
  /* restore the original terminal attributes */
  tcsetattr(fileno(stdin), TCSANOW, &orig_term_attr);
  
  return Py_BuildValue("i", character);
}

static PyMethodDef UbkeyMethods[] = {
  {"getch", getch, METH_VARARGS, "Returns key pressed, otherwise -1."},
  {NULL, NULL, 0, NULL}
};

PyMODINIT_FUNC initubkey(void) {
  (void)Py_InitModule("ubkey", UbkeyMethods);
}
