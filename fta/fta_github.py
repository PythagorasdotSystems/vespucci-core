from github import Github
from urllib.parse import urlparse


class FtaGithub:

    def __init__(self):
        self.git = Github()


    def __parse_github_url_path(self, url):
        url_parsed = urlparse(url)
        url_path_list = list(filter(None, url_parsed.path.rsplit('/')))
        
        return url_path_list


    def github_features(self, url):
        git_url_path = self.__parse_github_url_path(url)
        #git = Github()
        repo = self.git.get_user(git_url_path[0]).get_repo(git_url_path[1])

        features = {}
        features['stars'] = repo.watchers_count
        features['watchers'] = repo.subscribers_count
        features['forks'] = repo.forks_count
        features['open_issues'] = repo.open_issues_count
        #r=git.get_repo('/'.join(l)) # alternative
        return features

if __name__ == "__main__":
    print('FTA Github API example')
    
    git = FtaGithub()

    feats = git.github_features('https://github.com/litecoin-project/litecoin')

    print(feats)
