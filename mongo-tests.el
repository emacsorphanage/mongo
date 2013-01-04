;;; tests.el --- tests for mongo and bson

;; Copyright (C) 2012  Nic Ferrier

;; Author: Nic Ferrier <nferrier@ferrier.me.uk>
;; Keywords: lisp, data

;; This program is free software; you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

;;; Commentary:

;; Test code for mongo-db and bson stuff.

;;; Code:

(require 'bson)
(require 'cl)

(ert-deftest bson-datetime-int64-to-time ()
  "Test the bson datetime conversion."
  (let ((december-10-2010
         ;; I know this is the the mongo rep for this time because
         ;; I've pulled it from a packet dump
         (list #x40 #xa0 #x9f #xc3 #x2c #x01 #x00 #x00)))
    (should
     (equal
      "12/08/10"
      (format-time-string
       "%D"
       (bson-datetime-int64-to-time
        (reverse december-10-2010)))))))

(defmacro with-bson-test-buffer (datetime &rest body)
  "Execute BODY with the specified DATETIME in BSON format.

Clearly this function needs to be more generic, it will be as we
add tests to mongo.el."
  (declare (debug (sexp &rest form)))
  `(with-temp-buffer
     (insert
      (format "%c%s%c" ?\x09 "field_name" ?\x0))
     (mapc (lambda (c) (insert (format "%c" c))) ,datetime)
     (goto-char (point-min))
     (progn ,@body)))

(ert-deftest bson-datetime-deserialize ()
  "Test the de-serializing of BSON datetime."
  (let ((december-10-2010
         ;; I know this is the the mongo rep for this time because
         ;; I've pulled it from a packet dump
         (list #x40 #xa0 #x9f #xc3 #x2c #x01 #x00 #x00)))
    (with-bson-test-buffer december-10-2010
      (let ((bson-data (bson-deserialize-element)))
        ;; Check the value
        (should
         (equal
          "12/08/10"
          (format-time-string "%D" (cdr bson-data))))
        ;; Check the field name
        (should
         (equal
          "field_name" (car bson-data)))))))

(provide 'mongo-tests)

;;; tests.el ends here
