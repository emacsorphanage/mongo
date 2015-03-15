;;; bson.el --- Binary JSON serializer/deserializer

;; Copyright (C) 2011-2015  Tomohiro Matsuyama

;; Author: Tomohiro Matsuyama <m2ym.pub@gmail.com>
;; Version: 0.1
;; Keywords: convenience

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

;;; Code:

(require 'cl)

(defmacro bson-aif (test then &rest else)
  (declare (indent 2))
  `(let ((it ,test)) (if it ,then ,@else)))

(defmacro bson-awhen (test &rest body)
  (declare (indent 1))
  `(let ((it ,test)) (when it ,@body)))

(defmacro bson-with-temp-unibyte-buffer (&rest body)
  (declare (indent 0))
  `(with-temp-buffer (set-buffer-multibyte nil) ,@body))



(defsubst bson-alist-p (object)
  (and (listp object)
       (consp (first object))))

(defsubst bson-plist-p (object)
  (and (listp object)
       (symbolp (first object))))

(defmacro bson-evcase (object &rest clauses)
  (declare (indent 1))
  (let ((obj (gensym)))
    `(let ((,obj ,object))
       (cond
        ,@(loop for (value . body) in clauses
                collect `((eql ,obj ,value) ,@body))
        (t (error "bson-evcase failed: %s, %s"
                  ,obj ',(mapcar 'car clauses)))))))

(defsubst bson-type-of (object)
  (or (and (eq object t) 'boolean)
      (and (eq object nil) 'boolean)
      (and (vectorp object)
           (> (length object) 0)
           (symbolp (elt object 0))
           (let ((name (symbol-name (elt object 0))))
             (when (string-match "^cl-struct-\\(.+\\)$" name)
               (intern (match-string 1 name)))))
      (type-of object)))

(defmacro bson-etypecase (object &rest clauses)
  (declare (indent 1))
  `(ecase (bson-type-of ,object) ,@clauses))



(defmacro bson-document-dispatch (document &rest clauses)
  (declare (indent 1))
  `(cond
    ,@(loop for (type . body) in clauses
            collect
            (ecase type
              (hash-table `((hash-table-p ,document) ,@body))
              (alist      `((bson-alist-p ,document) ,@body))
              (plist      `((bson-plist-p ,document) ,@body))))))

(defun bson-document-size (document)
  (bson-document-dispatch document
    (hash-table (hash-table-count document))
    (alist      (length document))
    (plist      (/ (length document) 2))))

(defun bson-document-get (document key)
  (bson-document-dispatch document
    (hash-table (gethash key document))
    (alist      (cdr (assoc key document)))
    (plist      (plist-get document (intern key)))))

(defun bson-document-put (document key value)
  (bson-document-dispatch document
    (hash-table (puthash key value document))
    (alist      (bson-aif (assoc key document) 
                    (setcdr it value)
                  (push (cons key value) document)))
    (plist      (setq document (plist-put document (intern key) value))))
  document)

(defun bson-document-for (document function)
  (bson-document-dispatch document
    (hash-table (maphash function document))
    (alist      (loop for assoc in document
                      do (funcall function (car assoc) (cdr assoc))))
    (plist      (loop for key = (pop document)
                      for value = (pop document)
                      while key
                      do (funcall function (symbol-name key) value)))))

(defmacro* bson-document-do ((key value document &optional result) &rest body)
  (declare (indent 1))
  `(progn (bson-document-for ,document (lambda (,key ,value) ,@body))
          ,result))

(defun bson-document-to-hash-table (document)
  (let ((table (make-hash-table :test 'equal)))
    (bson-document-do (key value document table)
      (puthash key value table))))

(defun* bson-document-to-alist (document &aux alist)
  (bson-document-do (key value document alist)
    (push (cons key value) alist)))

(defun* bson-document-to-plist (document &aux plist)
  (bson-document-do (key value document plist)
    (setq plist `(,(intern key) ,value . ,plist))))



(defconst bson-marker-double       #x01)
(defconst bson-marker-string       #x02)
(defconst bson-marker-document     #x03)
(defconst bson-marker-array        #x04)
(defconst bson-marker-binary       #x05)
(defconst bson-marker-oid          #x07)
(defconst bson-marker-boolean      #x08)
(defconst bson-marker-datetime     #x09)
(defconst bson-marker-null         #x0a)
(defconst bson-marker-regexp       #x0b)
(defconst bson-marker-jscode       #x0d)
(defconst bson-marker-symbol       #x0e)
(defconst bson-marker-jscode/scope #x0f)
(defconst bson-marker-int32        #x10)
(defconst bson-marker-timestamp    #x11)
(defconst bson-marker-int64        #x12)
(defconst bson-marker-min-key      #xff)
(defconst bson-marker-max-key      #x7f)

(defstruct bson-oid string)

(defun bson-oid-to-hex-string (oid)
  (loop for byte across (bson-oid-string oid)
        collect (format "%02x" byte) into hex
        finally return (apply 'concat hex)))

(defun bson-oid-of-hex-string (hex-string)
  (loop for i from 0 below (length hex-string) by 2
        for hex = (substring hex-string i (+ i 2))
        collect (string-to-number hex 16) into bytes
        finally return (make-bson-oid :string (apply 'unibyte-string bytes))))

(defun bson-datetime-int64-to-time (byte-list)
  "Convert a 64 bit int as BYTE-LIST into an Elisp time."
  ;; Could do with some asserts to check byte-list
  (let ((calc-num
         (concat
          "16#"
          (mapconcat
           (lambda (x) (format "%02X" x))
           byte-list ""))))
    (list
     (calc-eval
      "rsh(and(idiv($,1000),16#ffff0000),16)"
      'rawnum
      calc-num)
     (calc-eval
      "and(idiv($,1000),16#ffff)"
      'rawnum
      calc-num))))

(defsubst bson-serialize-byte (byte)
  (insert-char byte 1))

(defsubst bson-serialize-int32 (int32)
  ;; TODO bigint
  (bson-serialize-byte (logand (lsh int32  -0) #xff))
  (bson-serialize-byte (logand (lsh int32  -8) #xff))
  (bson-serialize-byte (logand (lsh int32 -16) #xff))
  (bson-serialize-byte (logand (lsh int32 -24) #xff)))

(defsubst bson-serialize-int64 (int64)
  ;; TODO bigint
  (bson-serialize-byte (logand (lsh int64  -0) #xff))
  (bson-serialize-byte (logand (lsh int64  -8) #xff))
  (bson-serialize-byte (logand (lsh int64 -16) #xff))
  (bson-serialize-byte (logand (lsh int64 -24) #xff))
  (bson-serialize-byte (logand (lsh int64 -32) #xff))
  (bson-serialize-byte (logand (lsh int64 -40) #xff))
  (bson-serialize-byte (logand (lsh int64 -48) #xff))
  (bson-serialize-byte (logand (lsh int64 -56) #xff)))

(defsubst bson-serialize-double (double)
  (let (bytes (byte 0) (nbit 0))
    (flet ((serialize-bit (bit)
             (setq byte (logior (lsh byte 1) bit))
             (when (eq (incf nbit) 8)
               (push byte bytes)
               (setq byte 0
                     nbit 0)))
           (serialize-sign (sign)
             (serialize-bit (if (> sign 0) 0 1)))
           (serialize-exponent (exponent)
             (loop with bits
                   repeat 11 do
                   (push (mod exponent 2) bits)
                   (setq exponent (/ exponent 2))
                   finally (mapc 'serialize-bit bits)))
           (serialize-significand (significand)
             (loop repeat 52 do
                   (setq significand (* significand 2))
                   (if (< significand 1.0)
                       (serialize-bit 0)
                     (serialize-bit 1)
                     (decf significand)))))
      (let ((significand double)
            (exponent 0))
        (while (> significand 2.0)
          (setq significand (/ significand 2))
          (incf exponent))
        (serialize-sign (if (>= double 0.0) 1 -1))
        (serialize-exponent (+ exponent 1023))
        (serialize-significand (1- significand))))
    (assert (and (eq byte 0) (eq nbit 0)))
    (mapc 'bson-serialize-byte bytes)))

(defsubst bson-serialize-string (string)
  (bson-serialize-int32 (1+ (string-bytes string)))
  (insert string)
  (bson-serialize-byte #x00))

(defsubst bson-serialize-cstring (string)
  (insert string)
  (bson-serialize-byte #x00))

(defsubst bson-serialize-oid (oid)
  (insert (bson-oid-string oid)))

(defsubst bson-serialize-array (array)
  (bson-serialize-document
   (loop for i from 0
         for element across array
         collect `(,i . ,element))))

(defsubst bson-serialize-symbol (symbol)
  (bson-serialize-string (symbol-name symbol)))

(defsubst bson-serialize-boolean (boolean)
  (bson-serialize-byte (if boolean #x00 #x01)))

(defsubst bson-serialize-name (name)
  (bson-serialize-cstring
   (cond ((stringp name) name)
         ((numberp name) (number-to-string name))
         ((symbolp name) (symbol-name name))
         (t (error "invalid element name: %s" name)))))

(defsubst bson-serialize-marker (marker)
  (bson-serialize-byte marker))

(defun bson-serialize-element (name object)
  (macrolet ((serialize-element (marker function)
               `(progn
                  (bson-serialize-marker ,marker)
                  (bson-serialize-name name)
                  (,function object))))
    (bson-etypecase object
      (float      (serialize-element
                   bson-marker-double bson-serialize-double))
      (string     (serialize-element
                   bson-marker-string bson-serialize-string))
      (hash-table (serialize-element
                   bson-marker-document bson-serialize-document))
      (list       (serialize-element
                   bson-marker-document bson-serialize-document))
      (bson-oid   (serialize-element
                   bson-marker-oid bson-serialize-oid))
      (vector     (serialize-element
                   bson-marker-array bson-serialize-array))
      (boolean    (serialize-element
                   bson-marker-boolean bson-serialize-boolean))
      (symbol     (serialize-element
                   bson-marker-symbol bson-serialize-symbol))
      (integer    (serialize-element
                   bson-marker-int32 bson-serialize-int32)))))

(defun bson-serialize-document-1 (document)
  (bson-document-do (key value document)
    (bson-serialize-element key value)))

(defun bson-serialize-document (document)
  (let ((start (point)))
    (bson-serialize-document-1 document)
    (bson-serialize-byte #x00)
    (let ((end (point)))
      (save-excursion
        (goto-char start)
        (bson-serialize-int32 (+ (- end start) 4))))))

(defun* bson-serialize-document-to-buffer (document
                                           &optional (buffer (current-buffer)))
  (with-current-buffer buffer
    (bson-serialize-document document)))

(defun bson-serialize-document-to-string (document)
  (bson-with-temp-unibyte-buffer
    (bson-serialize-document document)
    (buffer-string)))

(defun* bson-serialize-document-to-stream (document
                                           &optional (stream standard-output))
  (let ((standard-output stream))
    (princ (bson-serialize-document-to-string document))))

(defun bson-serialize-document-to-process (document process)
  (process-send-string process (bson-serialize-document-to-string document)))



(defsubst bson-deserialize-byte ()
  (prog1 (char-after) (forward-char)))

(defsubst bson-deserialize-and-check-byte (expected)
  (let ((got (bson-deserialize-byte)))
    (assert (eq got expected))
    got))

(defsubst bson-deserialize-int32 ()
  (logior (lsh (bson-deserialize-byte)  0)
          (lsh (bson-deserialize-byte)  8)
          (lsh (bson-deserialize-byte) 16)
          (lsh (bson-deserialize-byte) 24)))

(defsubst bson-deserialize-int64 ()
  ;; FIXME: this probably needs to be done using calc or bigint
  (logior (lsh (bson-deserialize-byte)  0)
          (lsh (bson-deserialize-byte)  8)
          (lsh (bson-deserialize-byte) 16)
          (lsh (bson-deserialize-byte) 24)
          (lsh (bson-deserialize-byte) 32)
          (lsh (bson-deserialize-byte) 40)
          (lsh (bson-deserialize-byte) 48)
          (lsh (bson-deserialize-byte) 56)))

(defsubst bson-deserialize-double ()
  (let ((bytes (nreverse (loop repeat 8 collect (bson-deserialize-byte))))
        (nbit 0))
    (flet ((deserialize-bit ()
             (prog1 (logand (lsh (car bytes) (- nbit 7)) 1)
               (when (eq (incf nbit) 8)
                 (pop bytes)
                 (setq nbit 0))))
           (deserialize-sign ()
             (if (zerop (deserialize-bit)) 1 -1))
           (deserialize-exponent ()
             (loop for i from 10 downto 0
                   sum (lsh (deserialize-bit) i)))
           (deserialize-significand ()
             (loop
                with bits = (nreverse
                             (loop repeat 52 collect (deserialize-bit)))
                   with significand = 0.0
                   for bit in bits
                   if (eq bit 1)
                   do (incf significand)
                   do (setq significand (/ significand 2))
                   finally return significand)))
      (let* ((sign (deserialize-sign))
             (exponent (deserialize-exponent))
             (significand (deserialize-significand)))
        (* sign
           (1+ significand)
           (expt 2 (- exponent 1023)))))))

(defsubst bson-deserialize-string ()
  (let* ((length (bson-deserialize-int32))
         (start (point)))
    (forward-char (1- length))
    (prog1 (buffer-substring-no-properties start (point))
      (bson-deserialize-and-check-byte #x00))))

(defsubst bson-deserialize-cstring ()
  (let ((start (point)))
    (search-forward (string #x00))
    (forward-char -1)
    (prog1 (buffer-substring-no-properties start (point))
      (bson-deserialize-and-check-byte #x00))))

(defsubst bson-deserialize-datetime ()
  (let* ((bytes
          (loop repeat 8
             collect (bson-deserialize-byte))))
    (bson-datetime-int64-to-time (reverse bytes))))

(defsubst bson-deserialize-oid ()
  (let* ((bytes (loop repeat 12
                      collect (bson-deserialize-byte)))
         (string (apply 'unibyte-string bytes)))
    (make-bson-oid :string string)))

(defsubst bson-deserialize-array ()
  (let* ((document (bson-deserialize-document))
         (vector (make-vector (bson-document-size document) nil))
         (index 0))
    (bson-document-do (key value document vector)
      (declare (ignore key))
      (aset vector index value)
      (incf index))))

(defsubst bson-deserialize-binary ()
  (let* ((size (bson-deserialize-int32))
         (subtype (bson-deserialize-byte))
         (start (point)))
    (goto-char (+ start size))
    (list
     subtype
     (buffer-substring-no-properties start (point)))))

(defsubst bson-deserialize-symbol ()
  (intern (bson-deserialize-string)))

(defsubst bson-deserialize-boolean ()
  (ecase (bson-deserialize-byte)
    (#x00 t)
    (#x01 nil)))

(defsubst bson-deserialize-name ()
  (bson-deserialize-cstring))

(defsubst bson-deserialize-marker ()
  (bson-deserialize-byte))

(defsubst bson-deserialize-and-check-marker (expected)
  (let ((got (bson-deserialize-marker)))
    (assert (eq got expected))
    got))

(defun bson-deserialize-element ()
  (let* ((marker (bson-deserialize-marker))
         (name (bson-deserialize-name)))
    (cons name
          (bson-evcase marker
            (bson-marker-null     nil)
            (bson-marker-datetime (bson-deserialize-datetime))
            (bson-marker-double   (bson-deserialize-double))
            (bson-marker-string   (bson-deserialize-string))
            (bson-marker-binary   (bson-deserialize-binary))
            (bson-marker-document (bson-deserialize-document))
            (bson-marker-array    (bson-deserialize-array))
            (bson-marker-oid      (bson-deserialize-oid))
            (bson-marker-boolean  (bson-deserialize-boolean))
            (bson-marker-symbol   (bson-deserialize-symbol))
            (bson-marker-int32    (bson-deserialize-int32))))))

(defun bson-deserialize-document-1 (bound)
  (do ((document '()))
      ((or (>= (point) bound)
           (eq (char-after) #x00))
       (nreverse document))
    (destructuring-bind (key . value)
        (bson-deserialize-element)
      (push (cons key value) document))))

(defun bson-deserialize-document ()
  (let ((length (bson-deserialize-int32)))
    (prog1 (bson-deserialize-document-1 (+ (point) length -4))
      (bson-deserialize-and-check-byte #x00))))

(defun* bson-deserialize-document-from-buffer ((buffer (current-buffer)))
  (with-current-buffer buffer
    (bson-deserialize-document)))

(defun bson-deserialize-document-from-string (string)
  (bson-with-temp-unibyte-buffer
    (insert string)
    (goto-char (point-min))
    (bson-deserialize-document)))

(provide 'bson)
;;; bson.el ends here
