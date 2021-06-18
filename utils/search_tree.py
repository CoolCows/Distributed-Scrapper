class SearchTree():
    def __init__(self, root, depth:int) -> None:
        self.root = root
        self._depths = {root: depth}
        self._graph = dict()
        self._total_urls = 1
        self._updated = set()
        self.completed = False
    
    def pending_update(self, key):
        return key in self._depths and key not in self._updated

    def update(self, url, url_list):
        self._updated.add(url)
        if self._depths[url] == 1:
            self.completed = self.search_tree_completed()
            return []

        pending = []
        self._graph[url] = url_list
        for urlx in url_list:
            if not urlx in self._depths:
                self._depths[urlx] = self._depths[url] - 1
                pending.append(urlx)
        
        self._total_urls += len(pending)
        return pending
    
    def search_tree_completed(self) -> bool:
        queue = [self.root]
        visited = set()
        if self._depths[self.root] == 1:
            return True 
        
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
                    return False
        return True
