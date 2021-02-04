import matplotlib.pyplot as plt
import dbQueries



# graphing top genres with revenue
result_json = dbQueries.getTopGenres(10)
genres = []
count = []
for val in result_json:
    genres.append(val['genres'])
    count.append( val['noMovies'])


fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(genres,count,color = 'purple')
ax.set_ylabel('Count of movies')
plt.xlabel('genres')
plt.ylabel('count')
ax.set_title('Most popular genres of movies by revenue')
plt.show()


#
# graphing top genres with profit
result_json = dbQueries.getTopGenresByProfit(10)
genres = []
count = []
for val in result_json:
    genres.append(val['genres'])
    count.append( val['noMovies'])
fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
ax.bar(genres,count,color = 'purple')
ax.set_ylabel('Count of movies')
ax.set_title('Most popular genres of movies by profit')
plt.show()
print(result_json)
#
# # graphing top actors with revenue
# actors_json = dbQueries.getTopActorsGeneratingHighestRevenue(10)
# actor = []
# revenue = []
# for val in actors_json:
#     actor.append(val['actor'])
#     revenue.append( val['avgRevenue'])
# fig = plt.figure()
# ax = fig.add_axes([0,0,1,1])
# ax.bar(actor,revenue,color = 'orange')
# ax.set_ylabel('revenue from the movies')
# ax.set_title('Most popular actors')
# plt.show()
