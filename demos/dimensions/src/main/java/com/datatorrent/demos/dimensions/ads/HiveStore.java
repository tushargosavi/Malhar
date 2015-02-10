/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads;

import com.datatorrent.contrib.hds.HDSFileAccess;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HiveStore implements HDSFileAccess
{
  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final Logger logger = LoggerFactory.getLogger(HiveStore.class);

  protected String filepath;

  public String getFilepath()
  {
    return filepath;
  }

  public void setFilepath(String filepath)
  {
    this.filepath = filepath;
  }

  public HiveStore()
  {
    super();
  }


}