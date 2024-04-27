/*
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

package org.apache.druid.guice;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 */
public class ConditionalMultibindTest
{

  private static final String ANIMAL_TYPE = "animal.type";

  private Properties props;

  @Before
  public void setUp()
  {
    props = new Properties();
  }

  @Test
  public void testMultiConditionalBind_cat()
  {
    props.setProperty("animal.type", "cat");

    Injector injector = Guice.createInjector(new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        ConditionalMultibind.create(props, binder, Animal.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("cat"), Cat.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("dog"), Dog.class);
      }
    });

    Set<Animal> animalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }));

    Assert.assertEquals(1, animalSet.size());
    Assert.assertEquals(animalSet, ImmutableSet.<Animal>of(new Cat()));
  }

  @Test
  public void testMultiConditionalBind_cat_dog()
  {
    props.setProperty("animal.type", "pets");

    Injector injector = Guice.createInjector(new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        ConditionalMultibind.create(props, binder, Animal.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Cat.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Dog.class);
      }
    });

    Set<Animal> animalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }));

    Assert.assertEquals(2, animalSet.size());
    Assert.assertEquals(animalSet, ImmutableSet.of(new Cat(), new Dog()));
  }

  @Test
  public void testMultiConditionalBind_cat_dog_non_continuous_syntax()
  {
    props.setProperty("animal.type", "pets");

    Injector injector = Guice.createInjector(new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        ConditionalMultibind.create(props, binder, Animal.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Cat.class);

        ConditionalMultibind.create(props, binder, Animal.class)
                            .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Dog.class);

      }
    });

    Set<Animal> animalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }));

    Assert.assertEquals(2, animalSet.size());
    Assert.assertEquals(animalSet, ImmutableSet.of(new Cat(), new Dog()));
  }

  @Test
  public void testMultiConditionalBind_multiple_modules()
  {
    props.setProperty("animal.type", "pets");

    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Cat.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Dog.class);
          }
        },
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("not_match"), Tiger.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Fish.class);
          }
        }
    );

    Set<Animal> animalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }));

    Assert.assertEquals(3, animalSet.size());
    Assert.assertEquals(animalSet, ImmutableSet.of(new Cat(), new Dog(), new Fish()));
  }

  @Test
  public void testMultiConditionalBind_multiple_modules_with_annotation()
  {
    props.setProperty("animal.type", "pets");

    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class, SanDiego.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Cat.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Dog.class);
          }
        },
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class, SanDiego.class)
                                .addBinding(new Bird())
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Tiger.class);

            ConditionalMultibind.create(props, binder, Animal.class, SanJose.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Fish.class);
          }
        }
    );

    Set<Animal> animalSet_1 = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }, SanDiego.class));
    Assert.assertEquals(4, animalSet_1.size());
    Assert.assertEquals(animalSet_1, ImmutableSet.of(new Bird(), new Cat(), new Dog(), new Tiger()));

    Set<Animal> animalSet_2 = injector.getInstance(Key.get(new TypeLiteral<Set<Animal>>()
    {
    }, SanJose.class));
    Assert.assertEquals(1, animalSet_2.size());
    Assert.assertEquals(animalSet_2, ImmutableSet.<Animal>of(new Fish()));
  }

  @Test
  public void testMultiConditionalBind_inject()
  {
    props.setProperty("animal.type", "pets");

    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class)
                                .addBinding(Bird.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Cat.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Dog.class);
          }
        },
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind.create(props, binder, Animal.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("not_match"), Tiger.class)
                                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), Fish.class);
          }
        }
    );

    PetShotAvails shop = new PetShotAvails();
    injector.injectMembers(shop);

    Assert.assertEquals(4, shop.animals.size());
    Assert.assertEquals(shop.animals, ImmutableSet.of(new Bird(), new Cat(), new Dog(), new Fish()));
  }

  @Test
  public void testMultiConditionalBind_typeLiteral()
  {
    props.setProperty("animal.type", "pets");

    final Set<Animal> set1 = ImmutableSet.of(new Dog(), new Tiger());
    final Set<Animal> set2 = ImmutableSet.of(new Cat(), new Fish());
    final Set<Animal> set3 = ImmutableSet.of(new Cat());
    final Set<Animal> union = new HashSet<>();
    union.addAll(set1);
    union.addAll(set2);

    final Zoo<Animal> zoo1 = new Zoo<>(set1);
    final Zoo<Animal> zoo2 = new Zoo<>();

    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind
                .create(props, binder, new TypeLiteral<Set<Animal>>() {})
                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), set1)
                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), set2);

            ConditionalMultibind
                .create(props, binder, new TypeLiteral<Zoo<Animal>>() {})
                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), zoo1);
          }
        },
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            ConditionalMultibind
                .create(props, binder, new TypeLiteral<Set<Animal>>() {})
                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), set3);

            ConditionalMultibind
                .create(props, binder, new TypeLiteral<Set<Animal>>() {}, SanDiego.class)
                .addConditionBinding(ANIMAL_TYPE, Predicates.equalTo("pets"), union);

            ConditionalMultibind
                .create(props, binder, new TypeLiteral<Zoo<Animal>>() {})
                .addBinding(new TypeLiteral<Zoo<Animal>>() {});

          }
        }
    );

    Set<Set<Animal>> actualAnimalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Set<Animal>>>() {}));
    Assert.assertEquals(3, actualAnimalSet.size());
    Assert.assertEquals(ImmutableSet.of(set1, set2, set3), actualAnimalSet);

    actualAnimalSet = injector.getInstance(Key.get(new TypeLiteral<Set<Set<Animal>>>() {}, SanDiego.class));
    Assert.assertEquals(1, actualAnimalSet.size());
    Assert.assertEquals(ImmutableSet.of(union), actualAnimalSet);

    final Set<Zoo<Animal>> actualZooSet = injector.getInstance(Key.get(new TypeLiteral<Set<Zoo<Animal>>>() {}));
    Assert.assertEquals(2, actualZooSet.size());
    Assert.assertEquals(ImmutableSet.of(zoo1, zoo2), actualZooSet);
  }

  abstract static class Animal
  {
    private final String type;

    Animal(String type)
    {
      this.type = type;
    }

    @Override
    public String toString()
    {
      return "Animal{" +
             "type='" + type + '\'' +
             '}';
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Animal animal = (Animal) o;

      return type != null ? type.equals(animal.type) : animal.type == null;
    }

    @Override
    public int hashCode()
    {
      return type != null ? type.hashCode() : 0;
    }
  }

  static class PetShotAvails
  {
    @Inject
    Set<Animal> animals;
  }

  static class Dog extends Animal
  {
    Dog()
    {
      super("dog");
    }
  }

  static class Cat extends Animal
  {
    Cat()
    {
      super("cat");
    }
  }

  static class Fish extends Animal
  {
    Fish()
    {
      super("fish");
    }
  }

  static class Tiger extends Animal
  {
    Tiger()
    {
      super("tiger");
    }
  }

  static class Bird extends Animal
  {
    Bird()
    {
      super("bird");
    }
  }

  static class Zoo<T>
  {
    Set<T> animals;

    public Zoo()
    {
      animals = new HashSet<>();
    }

    public Zoo(Set<T> animals)
    {
      this.animals = animals;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Zoo<?> zoo = (Zoo<?>) o;

      return animals != null ? animals.equals(zoo.animals) : zoo.animals == null;
    }

    @Override
    public int hashCode()
    {
      return animals != null ? animals.hashCode() : 0;
    }

    @Override
    public String toString()
    {
      return "Zoo{" +
             "animals=" + animals +
             '}';
    }
  }

  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface SanDiego
  {
  }

  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @BindingAnnotation
  @interface SanJose
  {
  }

}
