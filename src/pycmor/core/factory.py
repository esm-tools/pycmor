import inspect


class MetaFactory(type):
    _registry = {}

    def __new__(cls, name, bases, class_dict):
        class_dict["_registry"] = {}
        klass = super().__new__(cls, name, bases, class_dict)
        return klass

    def __init__(cls, name, bases, class_dict):
        super().__init__(name, bases, class_dict)

        if not hasattr(cls, "__abstractmethods__"):
            for base in bases:
                if isinstance(base, MetaFactory):
                    # Strip the base product name from the subclass name
                    base_name = base.__name__
                    stripped_name = name.replace(base_name, "")
                    base._registry[stripped_name] = cls


def create_factory(klass):
    """Factory factory"""  # factory for factories because why not

    class KlassFactory:
        @staticmethod
        def _retrieve_from_registry(subclass_type):
            if subclass_type not in klass._registry:
                raise ValueError(
                    f"No subclass {subclass_type} registered for {klass.__name__}"
                )
            return klass._registry[subclass_type]

        @staticmethod
        def get(subclass_type):
            return KlassFactory._retrieve_from_registry(subclass_type)

        # #####################################################################
        # TODO(PG): Actually remove these if they are not needed
        #
        # These might just go away if the interspection method works as expected
        # and we can call the "generated" from_foo methods directly from the
        # factory.
        #
        # #####################################################################
        @staticmethod
        def create(subclass_type, **kwargs):
            klass_instance = KlassFactory._retrieve_from_registry(subclass_type)
            return klass_instance(**kwargs)

        @staticmethod
        def from_dict(subclass_type, data):
            klass_instance = KlassFactory._retrieve_from_registry(subclass_type)
            return klass_instance.from_dict(data)

        # FIXME(PG): This doesn't work yet...
        @classmethod
        def _introspect_and_create_methods(cls, klass):
            for name, method in inspect.getmembers(klass, predicate=inspect.isfunction):
                if isinstance(method, property):
                    continue
                if name.startswith("__") and name.endswith("__"):
                    continue  # Skip special methods

                def create_factory_method(method_name):
                    @staticmethod
                    def factory_method(subclass_type, *args, **kwargs):
                        klass_instance = KlassFactory._retrieve_from_registry(
                            subclass_type
                        )
                        return getattr(klass_instance, method_name)(*args, **kwargs)

                    return factory_method

                setattr(cls, name, create_factory_method(name))
            return cls

    # Dynamically set the class name:
    KlassFactory.__name__ = f"{klass.__name__}Factory"
    KlassFactory.__repr__ = lambda self: f"{KlassFactory.__name__}()"
    # Set constructor methods
    # factory = KlassFactory._introspect_and_create_methods(klass)
    return KlassFactory
