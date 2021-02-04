import praw

reddit = praw.Reddit(
    client_id="yi_6eIYF8o2HLQ",
    client_secret="QgpuuStsvpvz91NGRBIjwl0KSjvL3g",
    password="reddit1234",
    user_agent="test",
    username="elnaz_di"
)

subreddit = reddit.subreddit('MovieDetails')
hot_python = subreddit.hot(limit=5)

# for submission in hot_python:
#    print(submission)

# for submission in hot_python:
#    print(submission.title)

for submission in hot_python:
    if not submission.stickied:
        print('Title : {}, ups : {}, downs : {}, Have we visited : {}'.format(
            submission.title, submission.ups, submission.downs, submission.visited))


# subreddit.unsubscribe()
# subreddit.subscribe()

comments = submission.comments.list()
for comment in comments:
    print(20*'-')
    print("Parent ID: ", comment.parent())
    print("Comment ID: ", comment.id)
    print(comment.body)
    if len(comment.replies) > 0:
        for reply in comment.replies:
            print("REPLY", reply.body)
