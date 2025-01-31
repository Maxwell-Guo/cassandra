/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class Resources
{
    /**
     * Construct a chain of resource parents starting with the resource and ending with the root.
     *
     * @param resource The starting point.
     * @return list of resource in the chain form start to the root.
     */
    public static List<? extends IResource> chain(IResource resource)
    {
        return chain(resource, (r) -> true);
    }

    /**
     * Construct a chain of resource parents starting with the resource and ending with the root. Only resources which
     * satisfy the supplied predicate will be included.
     *
     * @param resource The starting point.
     * @param filter can be used to omit specific resources from the chain
     * @return list of resource in the chain form start to the root.
     */
    public static List<? extends IResource> chain(IResource resource, Predicate<IResource> filter)
    {

        List<IResource> chain = new ArrayList<>(4);
        while (true)
        {
            if (filter.test(resource))
                chain.add(resource);
            if (!resource.hasParent())
                break;
            resource = resource.getParent();
        }
        return chain;
    }

    /**
     * Creates an IResource instance from its external name.
     * Resource implementation class is inferred by matching against the known IResource
     * impls' root level resources.
     * @param name external name to create IResource from
     * @return an IResource instance created from the name
     */
    public static IResource fromName(String name)
    {
        if (name.startsWith(RoleResource.root().getName()))
            return RoleResource.fromName(name);
        else if (name.startsWith(DataResource.root().getName()))
            return DataResource.fromName(name);
        else if (name.startsWith(FunctionResource.root().getName()))
            return FunctionResource.fromName(name);
        else if (name.startsWith(JMXResource.root().getName()))
            return JMXResource.fromName(name);
        else
            throw new IllegalArgumentException(String.format("Name %s is not valid for any resource type", name));
    }
}
