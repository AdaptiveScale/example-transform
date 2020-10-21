/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.hydrator.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.soulwing.rot13.Rot13;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class SimpleMaskTest {
    private static final Schema INPUT = Schema.recordOf("input",
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("lastName", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("sex", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("nationality", Schema.of(Schema.Type.STRING)));

    @Test
    public void testMyMask() throws Exception {
        SimpleMask.Config config = new SimpleMask.Config("lastName", INPUT.toString());
        SimpleMask simpleMask = new SimpleMask(config);
        simpleMask.initialize(null);
        MockEmitter<StructuredRecord> emitter = new MockEmitter<>();


        simpleMask.transform(StructuredRecord.builder(INPUT)
                .set("name", "Flakrim")
                .set("lastName", "Jusufi")
                .set("sex", "Male")
                .set("nationality", "Albanian")
                .build(), emitter);

        Assert.assertEquals("Flakrim", emitter.getEmitted().get(0).get("name"));
        Assert.assertEquals(Rot13.rotate("Jusufi"), emitter.getEmitted().get(0).get("lastName"));
        Assert.assertEquals("Male", emitter.getEmitted().get(0).get("sex"));
        Assert.assertEquals("Albanian", emitter.getEmitted().get(0).get("nationality"));
    }
}
