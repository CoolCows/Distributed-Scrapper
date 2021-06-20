from collections import deque


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
            if self.completed:
                print(f"Completed when depht = 1 on {url}")
            return []

        pending = []
        for urlx in url_set:
            if not urlx in self._depths:
                self._depths[urlx] = self._depths[url] - 1
                pending.append(urlx)
        
        if len(pending) == 0:
            self._depths[url] = 1
            self.unexplored = self.search_tree_completed()
            self.completed =  len(self.unexplored) == 0
            if self.completed:
                print(f"Completed when no more to explore on {url}")
            return []

        self._graph[url] = url_set
        self._total_urls += len(pending)
        return pending
    
    def search_tree_completed(self) -> bool:
        if self._depths[self.root] == 1:
            return []

        queue = deque([self.root]) 
        visited = set()
        unexplored = []
        while len(queue) > 0:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            try:
                for child in self._graph[node]:
                    queue.append(child)
            except KeyError:
                if node not in self._updated or self._depths[node] > 1:
                    unexplored.append(node)

        # print(f"Unexplored: {len(unexplored)}")
        return unexplored
    
    def __repr__(self) -> str:
        return f"SearchTree({self.root}, {self._depths[self.root]}, {self.completed}, {self.unexplored})"

    def visual(self, caché, basic = False):
        visual =  f"Search Tree Results{'(Completed)' if self.completed else ''}:\n"
        visual += f"Scraped {self._total_urls} total urls.\n"
        visual += f"Root: {self.root}\n"
        visual += f"Max Depht: {self._depths[self.root]}\n"
        
        if basic:
            return visual
        
        urls_html = dict()
        print([urlx for urlx in caché],len([urlx for urlx in caché]))
        for url in self._depths:
            urls_html[url] = caché[url][0]

        return visual, urls_html
