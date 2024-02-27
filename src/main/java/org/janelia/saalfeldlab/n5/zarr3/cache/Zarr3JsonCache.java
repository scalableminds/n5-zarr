package org.janelia.saalfeldlab.n5.zarr3.cache;

import com.google.gson.JsonElement;
import org.janelia.saalfeldlab.n5.cache.N5JsonCache;
import org.janelia.saalfeldlab.n5.cache.N5JsonCacheableContainer;
import org.janelia.saalfeldlab.n5.zarr3.Zarr3KeyValueReader;

public class Zarr3JsonCache extends N5JsonCache {

  public Zarr3JsonCache(final N5JsonCacheableContainer container) {
    super(container);
  }

  @Override
  public void updateCacheInfo(final String normalPathKey, final String normalCacheKey,
      final JsonElement uncachedAttributes) {

    N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
    if (cacheInfo == null) {
      addNewCacheInfo(normalPathKey, normalCacheKey, uncachedAttributes);
      return;
    }

		if (cacheInfo == emptyCacheInfo) {
			cacheInfo = newCacheInfo();
		}

    if (normalCacheKey != null) {
      final JsonElement attributesToCache = uncachedAttributes == null
          ? container.getAttributesFromContainer(normalPathKey, normalCacheKey)
          : uncachedAttributes;

      updateCacheAttributes(cacheInfo, normalCacheKey, attributesToCache);

      // if this path is a group, it it not a dataset
      // if this path is a dataset, it it not a group
      if (normalCacheKey.equals(Zarr3KeyValueReader.ZARR_JSON_FILE)) {
        if (container.isGroupFromAttributes(normalCacheKey, attributesToCache)) {
          updateCacheIsGroup(cacheInfo, true);
          updateCacheIsDataset(cacheInfo, false);
        } else if (container.isDatasetFromAttributes(normalCacheKey, attributesToCache)) {
          updateCacheIsGroup(cacheInfo, false);
          updateCacheIsDataset(cacheInfo, true);
        }
      }
    } else {
      updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
      updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
    }
    updateCache(normalPathKey, cacheInfo);
  }

  public N5CacheInfo forceAddNewCacheInfo(final String normalPathKey, final String normalCacheKey,
      final JsonElement uncachedAttributes,
      final boolean isGroup, final boolean isDataset) {

    // getting the current cache info is useful if it has a list of children
    N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
		if (cacheInfo == null || cacheInfo == emptyCacheInfo) {
			cacheInfo = newCacheInfo();
		}

    // initialize cache keys to null, those that exist will be set later
    // and having null's in the cache avoid backend calls for nonexisting files
    updateCacheAttributes(cacheInfo, Zarr3KeyValueReader.ZARR_JSON_FILE, null);

		if (normalCacheKey != null) {
			updateCacheAttributes(cacheInfo, normalCacheKey, uncachedAttributes);
		}

    updateCacheIsGroup(cacheInfo, isGroup);
    updateCacheIsDataset(cacheInfo, isDataset);
    updateCache(normalPathKey, cacheInfo);

    return cacheInfo;
  }

  @Override
  public boolean isDataset(final String normalPathKey, final String normalCacheKey) {

    N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
    if (cacheInfo == null) {
      addNewCacheInfo(normalPathKey, normalCacheKey, null);
      cacheInfo = getCacheInfo(normalPathKey);
    } else if (cacheInfo == emptyCacheInfo || cacheInfo.isGroup()) {
			return cacheInfo.isDataset();
		} else if (!cacheInfo.containsKey(Zarr3KeyValueReader.ZARR_JSON_FILE)) {
			// if the cache info is not tracking zarr.json, then we don't yet know
			// if there is a dataset at this path key
			updateCacheIsDataset(cacheInfo, container.isDatasetFromContainer(normalPathKey));
		}

    return cacheInfo.isDataset();
  }

  @Override
  public boolean isGroup(final String normalPathKey, final String normalCacheKey) {

    N5CacheInfo cacheInfo = getCacheInfo(normalPathKey);
    if (cacheInfo == null) {
      addNewCacheInfo(normalPathKey, normalCacheKey, null);
      cacheInfo = getCacheInfo(normalPathKey);
    } else if (cacheInfo == emptyCacheInfo || cacheInfo.isDataset()) {
			return cacheInfo.isGroup();
		} else if (!cacheInfo.containsKey(Zarr3KeyValueReader.ZARR_JSON_FILE)) {
			// if the cache info is not tracking zarr.json, then we don't yet know
			// if there is a group at this path key
			updateCacheIsGroup(cacheInfo, container.isGroupFromContainer(normalPathKey));
		}

    return cacheInfo.isGroup();
  }

}
