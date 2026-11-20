"""Union by size, iterative path compression, and separate canonical names."""


class IdentityMap:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}
        self._size: dict[str, int] = {}
        self._minimum: dict[str, str] = {}

    def _find(self, user: str) -> str:
        parent = self._parent
        root = user
        while parent[root] != root:
            root = parent[root]
        while user != root:
            previous = parent[user]
            parent[user] = root
            user = previous
        return root

    def union(self, first: str | None, second: str | None) -> None:
        if not first or not second:
            return
        for user in (first, second):
            if user not in self._parent:
                self._parent[user] = user
                self._size[user] = 1
                self._minimum[user] = user
        left, right = self._find(first), self._find(second)
        if left == right:
            return
        if self._size[left] < self._size[right]:
            left, right = right, left
        self._parent[right] = left
        self._size[left] += self._size.pop(right)
        self._minimum[left] = min(
            self._minimum[left], self._minimum.pop(right), key=lambda name: name.encode("utf-8")
        )

    def person(self, user: str | None) -> str | None:
        if user is None or user not in self._parent:
            return user
        return self._minimum[self._find(user)]
