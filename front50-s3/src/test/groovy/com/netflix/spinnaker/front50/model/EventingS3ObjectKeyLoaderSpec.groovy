/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.front50.model

import spock.lang.Specification;

class EventingS3ObjectKeyLoaderSpec extends Specification {
  def "should build object key"() {
    when:
    def keyWithObjectType = EventingS3ObjectKeyLoader.buildObjectKey(
      "my/root/",
      "my/root/tags/aws%3Aservergroup%3Amy_asg-v720/entity-tags-metadata.json"
    )

    then:
    keyWithObjectType.key == "aws:servergroup:my_asg-v720"
    keyWithObjectType.objectType == ObjectType.ENTITY_TAGS
  }
}

