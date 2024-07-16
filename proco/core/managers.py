from django.db import connections, models


class BaseQuerySet(models.query.QuerySet):
    """
    BaseQuerySet
        to handle queryset used in the app
        Inherits: `models.query.QuerySet`
    """

    def live(self):
        """
        live
            Method to fetch all active objects of the queryset entity
            :returns: all active instance for querySet entity
        """
        return self.filter(deleted__isnull=True)

    def modified(self):
        """
        modified
            Method to fetch all modified objects of the queryset entity from history model
            :returns: all modified instance for querySet entity
        """
        return self.exclude(history_type='-').exclude(history_type='~', deleted__isnull=False)

    def all_deleted(self, hard_deleted=False):
        """
        deleted
            Method to fetch deleted objects of the queryset entity from history model
            :returns: deleted instance for querySet entity
        """
        if hard_deleted:
            return self.filter(history_type='-')
        return self.filter(deleted__isnull=False)

    def count(self, approx=True):
        if approx and not self.query.where:
            cursor = connections[self.db].cursor()
            cursor.execute(
                'SELECT reltuples::int FROM pg_class WHERE relname = %s;',
                (self.model._meta.db_table,),
            )
            return cursor.fetchall()[0][0]
        else:
            return super().count()


class BaseManager(models.Manager):
    """
    BaseManager
        to manage BaseQuery instance
        Inherits: `models.Manager`
    """

    def get_queryset(self, model=None, only_live_records=True):
        """
        get_queryset
            Method to fetch BaseQuerySet instance
            :returns: BaseQuerySet instance for model using self._db
        """
        # Use specified model while fetching deleted or modified objects
        # If model not specified, use default model
        if model is None:
            model = self.model
        base_queryset = BaseQuerySet(model, using=self._db)
        # if specified model is History model then return all records from History model
        # otherwise return all active records from specified model
        return base_queryset.live() if only_live_records else base_queryset

    def modified(self):
        """
        modified
            Method to fetch all modified objects of the queryset entity from history model
            :returns: all modified instance for querySet entity
        """
        return self.get_queryset(model=self.model.history.model, only_live_records=False).modified()

    def all_deleted(self, hard_deleted=False):
        """
        deleted
            Method to fetch deleted objects of the queryset entity from history model
            :returns: deleted instance for querySet entity
        """
        if hard_deleted:
            return self.get_queryset(
                model=self.model.history.model,
                only_live_records=False,
            ).all_deleted(hard_deleted=True)

        return self.get_queryset(model=self.model, only_live_records=False).all_deleted()

    def all_records(self):
        """
        all_records
            Method to fetch all the objects i.e. deleted as well as live records
        :return: instance for queryset entity
        """
        return self.get_queryset(model=self.model, only_live_records=False)
