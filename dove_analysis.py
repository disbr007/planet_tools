
# scenes[fld_ovlp_perc] = scenes.apply(lambda x: get_overlap(x, percent_overlap=True), axis=1)
# scenes[fld_avg_ovlp_perc] = scenes[fld_ovlp_perc].apply(lambda x: get_average_ovlp(x))
# scenes[fld_ovlp_area] = scenes.apply(lambda x: get_overlap(x, scenes=scenes, within_strip=False,
#                                                            within_days=True, percent_overlap=False), axis=1)
# scenes[fld_avg_ovlp_area] = scenes[fld_ovlp_area].apply(lambda x: get_average_ovlp(x))
#
# scenes[fld_sqkm] = round((scenes.geometry.area / 1e6), 2)
#
# agg = scenes.groupby(fld_ins_name).agg({fld_iid: 'count',
#                                         fld_sqkm: 'mean',
#                                         # fld_avg_ovlp_perc: 'mean',
#                                         fld_avg_ovlp_area: 'mean'})
#
# plt.style.use('pycharm_blank')
#
# fig, ax = plt.subplots(1,1)
# # sid_scenes.plot(color='none', edgecolor='white', ax=ax)
# scene_gdf.plot(color='none', edgecolor='orange', ax=ax)
# scene_ovl.plot(color='none', ax=ax, edgecolor='red')

# fig.show()
