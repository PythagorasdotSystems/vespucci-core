from github import Github
from urllib.parse import urlparse


class FtaGithub:

    def __init__(self):
        self.git = Github()


    def __parse_github_url_path(self, url):
        url_parsed = urlparse(url)
        url_path_list = list(filter(None, url_parsed.path.rsplit('/')))
        
        return url_path_list
    
    
    #def __
    
    
    #o = urlparse('https://github.com/litecoin-project/litecoin')
    #l = list(filter(None, o.path.rsplit('/')))
    
    def github_stats(self, url):
        git_url_path = self.__parse_github_url_path(url)
        #git = Github()
        repo = self.git.get_user(git_url_path[0]).get_repo(git_url_path[1])
        
        stats = {}
        stats['stars'] = repo.watchers_count
        stats['watchers'] = repo.subscribers_count
        stats['forks'] = repo.forks_count
        stats['open_issues'] = repo.open_issues_count
        #r=git.get_repo('/'.join(l)) # alternative
        return stats
    
