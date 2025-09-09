======================================
Usage: ``pycmor`` Using auxiliary files
======================================

At times, your post-processing will require additional files beyond the actual data.
For example, say your are analyzing FESOM output, and need to know the computational mesh
in order to calculate transport across a particular edge. In Python, the common way to do this
is to use the ``pyfesom2`` library to load the mesh. For a ``Rule`` to be aware of the mesh, you
can use auxiliary files.


You can add additional files to your ``Rule`` objects by specifying them in the
``aux`` element of the rule. These files are loaded when the ``Rule`` object is
initialized, and can be accessed in your steps.

For example, consider the following YAML configuration::


  rules:
    - name: My First Rule
      aux:
        - name: My Aux Data
          path: /path/to/aux/data.csv


You can then access this in a step like so::

  def my_step(data, rule):
    aux_data = rule.aux["My Aux Data"]
    print(aux_data)
    return data

By default, the program assumes you just have a text file which you can
read in. However, you may also want to use something else. Here is how
you can include a FESOM mesh object representation in ``pyfesom2``::


  rules:
    - name: My Other Rule
      aux:
        - name: mesh
          path: /some/path/to/a/mesh
          loader: pyfesom2.read_mesh_data.load_mesh

In Python, you get back the already loaded mesh object.
