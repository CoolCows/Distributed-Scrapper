from logging import raiseExceptions


class SearchTree():
    def __init__(self, root, depth:int) -> None:
        self.root = root
        self._depths = {root: depth}
        self._graph = dict()
        self._total_urls = 1
        self._updated = set()
        self.completed = False
        self.unexplored = []
    
    def pending_update(self, key):
        return key in self._depths and key not in self._updated

    def update(self, url, url_set):
        self._updated.add(url)
        if self._depths[url] == 1:
            self.unexplored = self.search_tree_completed()
            self.completed = len(self.unexplored) == 0
            return []

        pending = []
        for urlx in url_set:
            if not urlx in self._depths:
                self._depths[urlx] = self._depths[url] - 1
                pending.append(urlx)
        
        if len(pending) == 0:
            self._depths[url] = 1
            self.unexplored = self.search_tree_completed()
            self.completed = len(self.unexplored) == 0
            return []

        self._graph[url] = url_set
        self._total_urls += len(pending)
        return pending
    
    def search_tree_completed(self) -> bool:
        if self._depths[self.root] == 1:
            return []

        queue = [self.root]
        visited = set()
        unexplored = []
        for node in queue:
            queue.pop(0)
            if node in visited:
                continue
            visited.add(node)
            try:
                for child in self._graph[node]:
                    queue.append(child)
            except KeyError:
                if self._depths[node] != 1:
                    unexplored.append(node)

        return unexplored
    
    def __repr__(self) -> str:
        return f"SearchTree({self.root}, {self._depths[self.root]}, {self.unexplored})"
