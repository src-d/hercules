from collections import defaultdict, namedtuple


class DevDay(
    namedtuple("DevDay", ("Commits", "Added", "Removed", "Changed", "Languages"))
):
    def add(self, dd: 'DevDay') -> 'DevDay':
        langs = defaultdict(lambda: [0] * 3)
        for key, val in self.Languages.items():
            for i in range(3):
                langs[key][i] += val[i]
        for key, val in dd.Languages.items():
            for i in range(3):
                langs[key][i] += val[i]
        return DevDay(
            Commits=self.Commits + dd.Commits,
            Added=self.Added + dd.Added,
            Removed=self.Removed + dd.Removed,
            Changed=self.Changed + dd.Changed,
            Languages=dict(langs),
        )


class ParallelDevData:
    def __init__(self):
        self.commits_rank = -1
        self.commits = -1
        self.lines_rank = -1
        self.lines = -1
        self.ownership_rank = -1
        self.ownership = -1
        self.couples_index = -1
        self.couples_cluster = -1
        self.commit_coocc_index = -1
        self.commit_coocc_cluster = -1

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self)
