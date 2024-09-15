/*
 * Copyright 2021 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.common.output;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@SuppressWarnings({ "checkstyle:Indentation" })
public class Event {

    private Map<String, String> event;

    public Event() {

    }

    @SuppressWarnings("unchecked")
    public Event(final String json) throws JSONException {
        event = new ConcurrentHashMap<>();
        final JSONObject jsonOb = new JSONObject(json);
        final Iterator<String> itr = jsonOb.keys();
        while (itr.hasNext()) {
            final String key = itr.next();
            event.put(key, jsonOb.getString(key));
        }
    }

    public Map<String, String> getEvent() {
        return event;
    }

    public void setEvent(final Map<String, String> event) {
        this.event = event;
    }

    @Override
    public String toString() {

        String evntstr = "";

        if (event != null) {
            for (final String key : event.keySet()) {
                evntstr = evntstr + key + ": " + event.get(key) + ", ";

            }
        }

        return evntstr;

    }

}
