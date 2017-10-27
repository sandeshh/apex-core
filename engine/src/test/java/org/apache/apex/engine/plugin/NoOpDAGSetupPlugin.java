/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.engine.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.api.plugin.DAGSetupEvent;
import org.apache.apex.api.plugin.DAGSetupPlugin;
import org.apache.apex.api.plugin.Plugin;

import static org.apache.apex.api.plugin.DAGSetupEvent.Type.PRE_VALIDATE_DAG;

/**
 * This is a test plugin to test whether this DAGSetupPlugin gets loaded through service loader.
 */
public class NoOpDAGSetupPlugin implements DAGSetupPlugin<DAGSetupPlugin.Context>, Plugin.EventHandler<DAGSetupEvent>
{
  public static boolean setupCalled = false;
  public static boolean teardownCalled = false;
  public static boolean handleCalled = false;

  private static final Logger LOG = LoggerFactory.getLogger(NoOpDAGSetupPlugin.class);

  @Override
  public void setup(Context context)
  {
    LOG.info("NoOpDAGSetupPlugin - setup");
    setupCalled = true;
    context.register(PRE_VALIDATE_DAG, this);
  }

  @Override
  public void teardown()
  {
    LOG.info("NoOpDAGSetupPlugin - teardown");
    teardownCalled = true;
  }

  @Override
  public void handle(DAGSetupEvent event)
  {
    LOG.info("NoOpDAGSetupPlugin - handle");
    if (event.getType() == PRE_VALIDATE_DAG) {
      handleCalled = true;
    }
  }
}
