/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2019  hops.io
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
import io.hops.metadata.hdfs.dal.ProvidedBlockReportTasksDataAccess;
import io.hops.metadata.hdfs.entity.ProvidedBlockReportTask;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.*;

public class ProvidedBlockReportTasksClusterj
        implements TablesDef.ProvidedBlockReportTasksTabDef,
        ProvidedBlockReportTasksDataAccess<ProvidedBlockReportTask> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ProvidedBlockReportTaskDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @Column(name = START_INDEX)
    long getStartIndex();

    void setStartIndex(long startIndex);

    @Column(name = END_INDEX)
    long getEndIndex();

    void setEndIndex(long startIndex);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public ProvidedBlockReportTask popTask() throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProvidedBlockReportTaskDTO> dtos = null;
    try {
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQuery<ProvidedBlockReportTaskDTO> query =
              session.createQuery(qb.createQueryDefinition(ProvidedBlockReportTaskDTO.class));
      query.setLimits(0, 1);
      dtos = query.getResultList();
      List<ProvidedBlockReportTask> list = convert(dtos);
      assert list.size() <= 1;
      ProvidedBlockReportTask task = null;
      if (list.size() == 1) {
        ProvidedBlockReportTaskDTO dto = dtos.get(0);
        task = convert(dto);
        session.deletePersistent(dto);
      }
      session.release(dtos);
      return task;
    } finally {
      if(dtos != null){
        session.release(dtos);
      }
    }
  }

  @Override
  public long count() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public List<ProvidedBlockReportTask> getAllTasks() throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProvidedBlockReportTaskDTO> dtos = null;
    try {
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQuery<ProvidedBlockReportTaskDTO> query =
              session.createQuery(qb.createQueryDefinition(ProvidedBlockReportTaskDTO.class));
      dtos = query.getResultList();
      List<ProvidedBlockReportTask> list = convert(dtos);
      session.release(dtos);
      return list;
    } finally {
      if (dtos != null){
        session.release(dtos);
      }
    }
  }

  @Override
  public void addTasks(List<ProvidedBlockReportTask> tasks) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<ProvidedBlockReportTaskDTO> dtos = createPersistable(session, tasks);
    try {
      session.makePersistentAll(dtos);
    } finally {
      session.release(dtos);
    }
  }

  @Override
  public void deleteAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    session.deletePersistentAll(ProvidedBlockReportTaskDTO.class);
  }

  protected List<ProvidedBlockReportTaskDTO> createPersistable(HopsSession session,
                                                               List<ProvidedBlockReportTask> tasks)
          throws StorageException {
    List<ProvidedBlockReportTaskDTO>  list = new ArrayList<>();
    for( ProvidedBlockReportTask task : tasks){
      list.add(convert(session, task));
    }
    return list;
  }

  protected List<ProvidedBlockReportTask> convert(List<ProvidedBlockReportTaskDTO> dtos) {
    List<ProvidedBlockReportTask> list = new ArrayList<>();
    for (ProvidedBlockReportTaskDTO dto : dtos) {
      list.add(convert(dto));
    }
    return list;
  }

  protected ProvidedBlockReportTask convert(ProvidedBlockReportTaskDTO dto) {
    ProvidedBlockReportTask task = new ProvidedBlockReportTask(
            dto.getId(),
            dto.getStartIndex(),
            dto.getEndIndex()
    );
    return task;
  }

  protected ProvidedBlockReportTaskDTO convert(HopsSession session,
                                               ProvidedBlockReportTask task)
          throws StorageException {
    ProvidedBlockReportTaskDTO dto = session.newInstance(ProvidedBlockReportTaskDTO.class);
    //do not set the id. It is Auto Incremented
    dto.setStartIndex(task.getStartIndex());
    dto.setEndIndex(task.getEndIndex());
    return dto;
  }
}
