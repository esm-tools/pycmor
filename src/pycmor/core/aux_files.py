"""
Auxiliary files that can be attached to a Rule
"""

from .utils import get_callable


class AuxiliaryFile:
    """
    A class to represent an auxiliary file.

    Attributes
    ----------
    name : str
        The name of the file.
    path : str
        The path to the file.
    loader : callable, optional
        A callable to load the file.
    loader_args : list, optional
        Arguments to pass to the loader.
    loader_kwargs : dict, optional
        Keyword arguments to pass to the loader.

    Methods
    -------
    load():
        Loads the file using the specified loader or reads the file content.
    from_dict(d):
        Creates an AuxiliaryFile instance from a dictionary.
    """

    def __init__(self, name, path, loader=None, loader_args=None, loader_kwargs=None):
        """
        Constructs all the necessary attributes for the AuxiliaryFile object.

        Parameters
        ----------
        name : str
            The name of the file.
        path : str
            The path to the file.
        loader : callable, optional
            A callable to load the file.
        loader_args : list, optional
            Arguments to pass to the loader.
        loader_kwargs : dict, optional
            Keyword arguments to pass to the loader.
        """
        self.name = name
        self.path = path
        self.loader = loader
        if loader_args is None:
            loader_args = []
        self.loader_args = loader_args
        if loader_kwargs is None:
            loader_kwargs = {}
        self.loader_kwargs = loader_kwargs

    def load(self):
        """
        Loads the file using the specified loader or reads the file content.

        Returns
        -------
        str
            The content of the file if no loader is specified.
        object
            The result of the loader if a loader is specified.
        """
        if self.loader is None:
            with open(self.path, "r") as f:
                return f.read()
        else:
            loader = get_callable(self.loader)
            return loader(self.path, *self.loader_args, **self.loader_kwargs)

    @classmethod
    def from_dict(cls, d):
        """
        Creates an AuxiliaryFile instance from a dictionary.

        Parameters
        ----------
        d : dict
            A dictionary containing the attributes of the AuxiliaryFile.

        Returns
        -------
        AuxiliaryFile
            An instance of AuxiliaryFile.
        """
        return cls(
            d["name"],
            d["path"],
            d.get("loader"),
            d.get("loader_args"),
            d.get("loader_kwargs"),
        )


# NOTE(PG): Think about this...maybe it should be a method of Rule...
def attach_files_to_rule(rule):
    """
    Attaches extra files to the rule

    Mutates
    -------
    rule :
        The Rule object is modified to include the loaded auxiliary files
    """
    loaded_aux = {}
    for aux_file_spec in rule.get("aux", []):
        aux_file = AuxiliaryFile.from_dict(aux_file_spec)
        loaded_aux[aux_file.name] = aux_file.load()
    rule.aux = loaded_aux
