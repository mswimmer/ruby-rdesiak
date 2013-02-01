require 'rubygems'
require 'riak'
require 'multi_json'
require 'time'
require 'digest/sha1'
require "test/unit"

class Rdesiak
  def initialize
    @riak = Riak::Client.new host: "localhost", pb_port: 11087, http_port: 11098
    @rdesiak = @riak.bucket("rdesiak")
  end

  def add_triple(vertex_in_label, edge_label, vertex_out_label, graph = '<>')
    edge_id = edge_id(edge_label)
    vi = add_vertex(vertex_in_label, nil, edge_id)
    vo = add_vertex(vertex_out_label, edge_id, nil)
    e = add_edge(edge_label, vi, vo)
  end

  def has_triple?(vertex_in_label, edge_label, vertex_out_label, graph = '<>')
    begin
      @rdesiak.get vertex_id(vertex_in_label)
      @rdesiak.get vertex_id(vertex_out_label)
      e = @rdesiak.get edge_id(edge_label)
      (e.data['vertices_in'].include? vertex_id(vertex_in_label)) && (e.data['vertices_out'].include? vertex_id(vertex_out_label))
    rescue Riak::HTTPFailedRequest => error
      false
    end
  end

  # Match a subgraph against our graph DB.
  # The query is merely a list of triples so we just check to see if every 
  # triple is in the graph.
  def has_subgraph?(q)
    q.each {|t| return false unless (has_triple? *t) }
    true
  end

  def query?(q, &blk)
    operation = nil
    q.each do |s|
      if s.class == Array
        partial_triple(*s, &blk)
      elsif s.class == Symbol
        operation = s
      end
    end
  end

  def edges_out(label, graph = '<>')
    adjacency(vertex_id(label), 'edges_out', graph)
  end

  def edges_in(label, graph = '<>')
    adjacency(vertex_id(label), 'edges_in', graph)
  end

  def vertices_out(label, graph = '<>')
    adjacency(edge_id(label), 'vertices_out', graph)
  end

  def vertices_out_by_id(edge_id, graph = '<>')
    adjacency(edge_id, 'vertices_out', graph)
  end

  def vertices_in(label, graph = '<>')
    adjacency(edge_id(label), 'vertices_in', graph)
  end

  def has_vertex?(vertex_label)
    @rdesiak.get vertex_id(vertex_label)
  end

  def edge(edge_label)
    edge_by_id(edge_id(edge_label)).data
  end

  def edge_by_id(edge_id)
    @rdesiak.get(edge_id).data
  end

  def edge_label_by_id(edge_id)
    edge_by_id(edge_id)['label']
  end

  def vertex(vertex_label)
    vertex_by_id(vertex_id(vertex_label))
  end

  def vertex_by_id(vertex_id)
    @rdesiak.get(vertex_id).data
  end

  def vertex_label_by_id(vertex_id)
    vertex_by_id(vertex_id)['label']
  end

  def has_edge?(edge_label)
    @rdesiak.get edge_id(edge_label)
  end

  def wipedb
    Riak.disable_list_keys_warnings = true
    @rdesiak.keys do |k|
      unless k.empty?
        @rdesiak.delete k[0]
      end
    end
  end

  private

  def adjacency(id, field, graph)
    o = @rdesiak.get(id)
    if o.data['graphs'].include? graph
      o.data[field]
    else
      []
    end
  end

  def adjacencies(id, field1, field2, graph)
    o = @rdesiak.get(id)
    yield o.data[field1], o.data[field2] if o.data['graphs'].include? graph
  end

  def partial_triple(vi, e, vo, graph = "<>")
    #puts "partial_triple #{vi.class}, #{e.class}, #{vo.class}"
    if    (vi.class == Symbol) && (e.class == Symbol) && (vo.class == Symbol)
      # do a map/reduce thingy to find all the edges and return these

#      all_edges.collect { |edge| 
#        map(map_phase).reduce(reduce_phase, :keep => true).run
#      }
      Riak.disable_list_keys_warnings = true
      @rdesiak.keys do |k|
        unless k.empty?
          puts "#{k} .. #{k[/^edge/]}"
          unless k[/^edge-/] == nil
            adjacencies(k, 'vertices_in', 'vertices_out', graph) { |evia, evoa|
              evia.each { |evi|
                evoa.each { |evo|
                  yield [evi, k, evo]
                }
              }
            }
          end
        end
      end
    elsif (vi.class == Symbol) && (e.class == Symbol) && (vo.class != Symbol)
      edges_in(vo, graph).each { |edge|
        adjacency(edge, 'vertices_in', graph).each { |vertex|
          yield [vertex_label_by_id(vertex), edge_label_by_id(edge), vo]
        }
      }
    elsif (vi.class == Symbol) && (e.class != Symbol) && (vo.class == Symbol)
      adjacencies(edge_id(e), 'vertices_in', 'vertices_out', graph) { |evia, evoa|
        evia.collect { |evi| 
          evoa.collect { |evo| 
            yield [vertex_label_by_id(evi), e, vertex_label_by_id(evo)] 
          }
        }
      }
    elsif (vi.class == Symbol) && (e.class != Symbol) && (vo.class != Symbol)
      evo = vertex_id(vo)
      e_id = edge_id(e)
      adjacencies(e_id, 'vertices_in', 'vertices_out', graph) { |evia, evoa|
        evia.each { |evi| yield [ vertex_label_by_id(evi), e, vo] } if (evoa.include? evo)
      }
    elsif (vi.class != Symbol) && (e.class == Symbol) && (vo.class == Symbol)
      evi = vertex_id(vi)
      edges_out(vi, graph).each { |edge|
        vertices_out_by_id(edge, graph).each { |evo|
          yield [vertex_label_by_id(evi), edge_label_by_id(edge), vertex_label_by_id(evo)]
        }
      }
    elsif (vi.class != Symbol) && (e.class == Symbol) && (vo.class != Symbol)
      a = edges_in(vo, graph).each { |edge| 
        adjacency(edge, 'vertices_in', graph).each { |vertex|
          yield [vi, edge_label_by_id(edge), vo] if (vertex_id(vi) == vertex)
        }
      }
    elsif (vi.class != Symbol) && (e.class != Symbol) && (vo.class == Symbol)
      adjacencies(edge_id(e), 'vertices_in', 'vertices_out', graph) { |evia, evoa|
        evi = vertex_id(vi)
        evoa.each { |evo| yield [vertex_label_by_id(evi), e, vertex_label_by_id(evo)] } if (evia.include? evi)
      }
    elsif (vi.class != Symbol) && (e.class != Symbol) && (vo.class != Symbol)
      adjacencies(edge_id(e), 'vertices_in', 'vertices_out', graph) { |evia, evoa|
        yield [vi, e, vo] if (evia.include? vertex_id(vi)) && (evoa.include? vertex_id(vo))
      }
    end
  end

  def add_vertex(label, edge_in = nil, edge_out = nil, graph = '<>')
    nobj(vertex_id(label), label, 'edges_in', edge_in, 'edges_out', edge_out, graph)
  end

  def add_edge(label, vertex_in, vertex_out, graph = '<>')
    nobj(edge_id(label), label, 'vertices_in', vertex_in, 'vertices_out', vertex_out, graph)
  end

  def nobj(id, label, d1l, d1d, d2l, d2d, graph)
    o = @rdesiak.get_or_new(id)
    o.data = {} unless o.data
    o.data[d1l] = add(o.data[d1l], d1d) unless d1d.nil?
    o.data[d2l] = add(o.data[d2l], d2d) unless d2d.nil?
    o.data['graphs'] = add(o.data['graphs'], graph) unless graph.nil?
    o.data['label'] = label
    #puts "Writing"
    #p o.data
    o.store
    id
  end

  def add(a, e)
    if a.nil?
      [e]
    else
      Set.new(a).add(e).to_a
    end
  end

  def edge_id(label)
    id(label, "edge")
  end

  def vertex_id(label)
    id(label, "vertex")
  end

  def id(label, type)
    sha1 = Digest::SHA1.new
    sha1.update label
    type + '-' + sha1.to_s
  end

  def all_edges
    map_phase = "function(v) { return v.key;}"
 
    reduce_phase = <<-EOF
function(v) {
  var r = {};
  for(var i in v) {
    for(var w in v[i]) {
      if(w in r) r[w] += v[i][w]; 
      else r[w] = v[i][w];
    }
  }
  return [r];
}
EOF
 
    results = @rdesiak.map(map_phase).reduce(reduce_phase, :keep => true).run
  end
end

class TestRdesiak < Test::Unit::TestCase
 
  def setup
    @rd = Rdesiak.new
    @rd.wipedb
    [ ['main', '0', 'parse'],
      ['parse', '1', 'execute'],
      ['main', '2', 'init'],
      ['main', '3', 'cleanup'],
      ['execute', '4', 'make_string'],
      ['execute', '5', 'printf'],
      ['init', '6', 'make_string'],
      ['main', '7', 'printf'],
      ['execute', '8', 'compare']
    ].each {|t| @rd.add_triple(t[0], t[1], t[2]) }
  end
 
  def teardown
    @rd.wipedb
  end
 
  def test_edges_out
    assert_equal(["edge-b6589fc6ab0dc82cf12099d1c2d40ab994e8410c",
 "edge-da4b9237bacccdf19c0760cab7aec4a8359010b0",
 "edge-77de68daecd823babbb58edb1c8e14d7106e83bb",
 "edge-902ba3cda1883801594b6e1b452790cc53948fda"], @rd.edges_out('main'))
  end
 
  def test_edges_in
    assert_nil(@rd.edges_in('main'))
  end

  def test_vertices_out
    assert_equal(["vertex-132aac8d60d18ce3cefd7562f58b62f87350132b"], @rd.vertices_out('4'))
  end

  def test_vertices_in
    assert_equal(["vertex-39ae89e0a135945a5e08d9340cd2eac771f228f6"], @rd.vertices_in('4'))
  end

  def test_has_triple
    assert(@rd.has_triple?('main', '0', 'parse'))
    assert(@rd.has_triple?('parse', '1', 'execute'))
    assert(@rd.has_triple?('execute', '5', 'printf'))
    assert(!@rd.has_triple?('main', '1', 'blah'))
  end

  def test_has_subgraph
    assert(@rd.has_subgraph?([ ['main', '0', 'parse'], 
                               ['parse', '1', 'execute'], 
                               ['execute', '5', 'printf']]))
    assert(! @rd.has_subgraph?([ ['main', '0', 'parse'], 
                                 ['parse', '1', 'execute'], 
                                 ['execute', '15', 'printf']]))
  end

  def test_query
    c = 0
    @rd.query?([[:s, :p, 'printf']]) { |triple|
      assert([
              ["execute", "5", "printf"], ["main", "7", "printf"]
             ].include? triple)
      c += 1
    }
    assert_equal(2, c)
    c = 0
    @rd.query?([[:s, '5', :o]]) { |triple|
      assert_equal(["execute", "5", "printf"], triple)
      c += 1
    }
    assert_equal(1, c)
    c = 0
    @rd.query?([['main', '2', :o]]) { |triple|
      assert_equal(["main", "2", "init"], triple)
      c += 1
    }
    assert_equal(1, c)
    c = 0
    @rd.query?([['main', :p, 'cleanup']]) { |triple|
      assert_equal(["main", "3", "cleanup"], triple)
      c += 1
    }
    assert_equal(1, c)
    c = 0
    @rd.query?([['main', :p, 'printf']]) { |tripel|
      assert_equal(["main", "7", "printf"], tripel)
      c += 1
    }
    c = 0
    @rd.query?([['main', :p, :o]]) { |tripel|
      assert([["main", "0", "parse"], ["main", "2", "init"], ["main", "3", "cleanup"], ["main", "7", "printf"]].include? tripel)
      c += 1
    }
    assert_equal(4, c)
    c = 0
    @rd.query?([[:s, '6', 'make_string']]) { |tripel|
      assert_equal(["init", "6", "make_string"], tripel)
      c += 1
    }
    assert_equal(1, c)
    c = 0
    @rd.query?([['main', '2', 'init']]) { |tripel| 
      assert_equal(['main', '2', 'init'], tripel)
      c += 1
    }
    assert_equal(1, c)
    c = 0
    @rd.query?([[:s, :p, :o]]) { |tripel|
      assert([ ['main', '0', 'parse'],
               ['parse', '1', 'execute'],
               ['main', '2', 'init'],
               ['main', '3', 'cleanup'],
               ['execute', '4', 'make_string'],
               ['execute', '5', 'printf'],
               ['init', '6', 'make_string'],
               ['main', '7', 'printf'],
               ['execute', '8', 'compare']
             ].include? tripel)
      c += 1
    }
    assert_equal(9, c)
  end

end

rd = Rdesiak.new
rd.wipedb
[ ['main', '0', 'parse'],
  ['parse', '1', 'execute'],
  ['main', '2', 'init'],
  ['main', '3', 'cleanup'],
  ['execute', '4', 'make_string'],
  ['execute', '5', 'printf'],
  ['init', '6', 'make_string'],
  ['main', '7', 'printf'],
  ['execute', '8', 'compare']
].each {|t| rd.add_triple(t[0], t[1], t[2]) }

#puts "query results:"
#p rd.query?([[:s, :p, 'printf']])
#p rd.query?([[:s, '5', :o]])
#rd.query?([['main', '2', 'init']]) {|t| p t}

#p rd.query?([['main', :p, 'printf']])
#p rd.query?([['main', :p, :o]])
#p rd.query?([[:s, '6', 'make_string']])
#rd.query?([[:s, :p, :o]])

#puts "query results:"
#p rd.query?([['main', '0', :x], :and, [:x, :p, :y], :and, [:y, '5', 'printf']])
