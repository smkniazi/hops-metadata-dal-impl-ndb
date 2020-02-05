/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2020  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.CloudBucketDataAccess;
import io.hops.metadata.hdfs.entity.CloudBucket;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.*;

public class CloudBucketsClusterj
        implements TablesDef.CloudBucketsTabDef,
        CloudBucketDataAccess<CloudBucket> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface CloudBucketDTO {

    @PrimaryKey
    @Column(name = ID)
    short getID();

    void setID(short bid);

    @Column(name = NAME)
    String getName();

    void setName(String name);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public int addBucket(String name) throws StorageException {
    HopsSession session = connector.obtainSession();
    CloudBucketDTO dto = null;
    try {
      dto = session.newInstance(CloudBucketDTO.class);
      dto.setName(name);
      session.makePersistent(dto);
      session.flush();
      return getID(name);
    } finally {
      session.release(dto);
    }
  }

  public int getID(String name) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CloudBucketDTO> dobj =  qb.createQueryDefinition
            (CloudBucketDTO.class);
    dobj.where(dobj.get("name").equal(dobj.param("param")));
    HopsQuery<CloudBucketDTO> query = session.createQuery(dobj);
    query.setParameter("param", name);
    List<CloudBucketDTO> results = query.getResultList();
    CloudBucket bucket = null;

    assert results.size() <= 1;

    if(results.size() == 1) {
      bucket = convert(results.get(0));
      session.release(results);
      return bucket.getID();
    } else {
      throw new StorageException("Bucket "+name+" not found");
    }
  }

  @Override
  public Map<String, CloudBucket> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    Map<String, CloudBucket> buckets = new HashMap<>();
    HopsQuery<CloudBucketDTO> query =
            session.createQuery(qb.createQueryDefinition(CloudBucketDTO.class));
    List<CloudBucketDTO> dtos = query.getResultList();
    for (CloudBucketDTO dto : dtos) {
      buckets.put(dto.getName(), convert(dto));
    }
    session.release(dtos);
    return buckets;
  }


  protected CloudBucket convert(CloudBucketDTO dto) {
    CloudBucket bucket = new CloudBucket(dto.getID(), dto.getName());
    return bucket;
  }

  protected CloudBucketDTO convert(HopsSession session,
                                   CloudBucket bucket)
          throws StorageException {
    CloudBucketDTO dto = session.newInstance(CloudBucketsClusterj.CloudBucketDTO.class);
    dto.setID(bucket.getID());
    dto.setName(bucket.getName());
    return dto;
  }
}
